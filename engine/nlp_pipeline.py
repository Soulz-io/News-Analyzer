"""NLP preprocessing pipeline for the News Analyzer engine.

For each fetched Article the pipeline produces an ArticleBrief containing:
  - Language detection
  - Translation to English (when needed)
  - Named-entity recognition (spaCy)
  - Keyword extraction (YAKE)
  - Sentiment analysis (VADER)
  - Extractive summarisation (sumy / LexRank)
  - Geo-tagging (GPE -> region mapping)
  - Intensity classification
  - BERTopic clustering (batch level)
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any

from .config import config
from .db import get_session, Article, ArticleBrief

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy-loaded heavy models
# ---------------------------------------------------------------------------

_spacy_nlp = None
_vader_analyzer = None
_yake_extractor = None
_topic_model = None

# Threat keyword set used for intensity classification
THREAT_KEYWORDS: set = {
    "war", "conflict", "attack", "missile", "bomb", "strike", "invasion",
    "troops", "military", "sanctions", "nuclear", "weapon", "terror",
    "terrorism", "hostage", "assassination", "coup", "martial law",
    "emergency", "escalation", "threat", "airstrikes", "drone",
    "casualties", "killed", "death toll", "explosion", "shelling",
    "genocide", "chemical", "biological", "blockade", "siege",
}

# GPE -> region mapping (lowercase keys)
GPE_REGION_MAP: Dict[str, str] = {
    # Middle East
    "israel": "middle-east", "palestine": "middle-east", "gaza": "middle-east",
    "iran": "middle-east", "iraq": "middle-east", "syria": "middle-east",
    "lebanon": "middle-east", "yemen": "middle-east", "saudi arabia": "middle-east",
    "jordan": "middle-east", "uae": "middle-east", "united arab emirates": "middle-east",
    "qatar": "middle-east", "kuwait": "middle-east", "bahrain": "middle-east",
    "oman": "middle-east", "turkey": "middle-east", "egypt": "middle-east",
    "west bank": "middle-east",
    # East Asia
    "china": "east-asia", "japan": "east-asia", "south korea": "east-asia",
    "north korea": "east-asia", "taiwan": "east-asia", "hong kong": "east-asia",
    "mongolia": "east-asia", "beijing": "east-asia", "tokyo": "east-asia",
    "seoul": "east-asia", "pyongyang": "east-asia", "taipei": "east-asia",
    "shanghai": "east-asia",
    # South Asia
    "india": "south-asia", "pakistan": "south-asia", "bangladesh": "south-asia",
    "sri lanka": "south-asia", "nepal": "south-asia", "afghanistan": "south-asia",
    "maldives": "south-asia", "bhutan": "south-asia",
    # Southeast Asia
    "vietnam": "southeast-asia", "thailand": "southeast-asia",
    "indonesia": "southeast-asia", "philippines": "southeast-asia",
    "malaysia": "southeast-asia", "singapore": "southeast-asia",
    "myanmar": "southeast-asia", "cambodia": "southeast-asia",
    "laos": "southeast-asia", "brunei": "southeast-asia",
    # Russia / CIS
    "russia": "russia-cis", "ukraine": "russia-cis", "belarus": "russia-cis",
    "kazakhstan": "russia-cis", "uzbekistan": "russia-cis",
    "georgia": "russia-cis", "armenia": "russia-cis", "azerbaijan": "russia-cis",
    "moscow": "russia-cis", "kyiv": "russia-cis", "crimea": "russia-cis",
    # Africa
    "nigeria": "africa", "south africa": "africa", "kenya": "africa",
    "ethiopia": "africa", "ghana": "africa", "tanzania": "africa",
    "uganda": "africa", "mozambique": "africa", "congo": "africa",
    "sudan": "africa", "somalia": "africa", "libya": "africa",
    "tunisia": "africa", "morocco": "africa", "algeria": "africa",
    "mali": "africa", "niger": "africa", "chad": "africa",
    "cameroon": "africa", "senegal": "africa", "zimbabwe": "africa",
    "rwanda": "africa", "burkina faso": "africa",
    # Latin America
    "brazil": "latam", "mexico": "latam", "argentina": "latam",
    "colombia": "latam", "chile": "latam", "peru": "latam",
    "venezuela": "latam", "cuba": "latam", "ecuador": "latam",
    "bolivia": "latam", "guatemala": "latam", "haiti": "latam",
    "honduras": "latam", "el salvador": "latam", "nicaragua": "latam",
    "panama": "latam", "costa rica": "latam", "uruguay": "latam",
    "paraguay": "latam",
    # Europe
    "germany": "europe", "france": "europe", "uk": "europe",
    "united kingdom": "europe", "britain": "europe", "italy": "europe",
    "spain": "europe", "poland": "europe", "romania": "europe",
    "netherlands": "europe", "belgium": "europe", "sweden": "europe",
    "portugal": "europe", "greece": "europe", "czech republic": "europe",
    "austria": "europe", "hungary": "europe", "switzerland": "europe",
    "ireland": "europe", "denmark": "europe", "finland": "europe",
    "norway": "europe", "serbia": "europe", "croatia": "europe",
    "bosnia": "europe", "kosovo": "europe", "london": "europe",
    "paris": "europe", "berlin": "europe", "brussels": "europe",
    # North America
    "united states": "north-america", "us": "north-america",
    "usa": "north-america", "canada": "north-america",
    "washington": "north-america", "new york": "north-america",
    "pentagon": "north-america", "white house": "north-america",
    "ottawa": "north-america",
}

VALID_REGIONS = {
    "middle-east", "east-asia", "south-asia", "southeast-asia",
    "russia-cis", "africa", "latam", "europe", "north-america", "global",
}


# ---------------------------------------------------------------------------
# Lazy loaders
# ---------------------------------------------------------------------------

def _get_spacy():
    """Load the spaCy model lazily."""
    global _spacy_nlp
    if _spacy_nlp is None:
        try:
            import spacy
            _spacy_nlp = spacy.load(config.spacy_model)
            logger.info("spaCy model '%s' loaded.", config.spacy_model)
        except OSError:
            logger.error(
                "spaCy model '%s' is not installed. "
                "Run: python -m spacy download %s",
                config.spacy_model,
                config.spacy_model,
            )
            raise
    return _spacy_nlp


def _get_vader():
    """Load VADER sentiment analyzer lazily."""
    global _vader_analyzer
    if _vader_analyzer is None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _vader_analyzer = SentimentIntensityAnalyzer()
        logger.info("VADER sentiment analyzer loaded.")
    return _vader_analyzer


def _get_yake():
    """Create a YAKE keyword extractor lazily."""
    global _yake_extractor
    if _yake_extractor is None:
        import yake
        _yake_extractor = yake.KeywordExtractor(
            lan="en",
            n=2,           # max n-gram size
            dedupLim=0.7,
            top=config.yake_top_keywords,
            features=None,
        )
        logger.info("YAKE keyword extractor loaded.")
    return _yake_extractor


def _get_topic_model():
    """Create or return the BERTopic model lazily."""
    global _topic_model
    if _topic_model is None:
        try:
            from bertopic import BERTopic
            _topic_model = BERTopic(
                language="english",
                calculate_probabilities=False,
                verbose=False,
            )
            logger.info("BERTopic model initialised.")
        except Exception:
            logger.exception("Failed to initialise BERTopic -- clustering disabled.")
            _topic_model = None
    return _topic_model


# ---------------------------------------------------------------------------
# Language detection & translation
# ---------------------------------------------------------------------------

def detect_language(text: str) -> str:
    """Detect the language of *text*.  Returns an ISO-639-1 code."""
    try:
        from fast_langdetect import detect
        result = detect(text, low_memory=True)
        lang = result.get("lang", "en") if isinstance(result, dict) else "en"
        return lang
    except Exception:
        logger.debug("Language detection failed -- defaulting to 'en'.")
        return "en"


def translate_to_english(text: str, source_lang: str) -> str:
    """Translate *text* from *source_lang* to English using argostranslate.

    Falls back to the original text when the required language pack is not
    installed.
    """
    if source_lang == "en":
        return text

    try:
        import argostranslate.translate
        installed = argostranslate.translate.get_installed_languages()

        src = None
        tgt = None
        for lang in installed:
            if lang.code == source_lang:
                src = lang
            if lang.code == "en":
                tgt = lang

        if src is None or tgt is None:
            logger.warning(
                "Argostranslate language pack '%s' -> 'en' not installed. "
                "Using original text.",
                source_lang,
            )
            return text

        translation = src.get_translation(tgt)
        if translation is None:
            logger.warning(
                "No translation path from '%s' to 'en'. Using original text.",
                source_lang,
            )
            return text

        translated = translation.translate(text)
        return translated

    except ImportError:
        logger.warning("argostranslate not installed -- skipping translation.")
        return text
    except Exception:
        logger.exception("Translation failed for lang=%s -- using original text.", source_lang)
        return text


# ---------------------------------------------------------------------------
# NER
# ---------------------------------------------------------------------------

def extract_entities(doc) -> Dict[str, List[str]]:
    """Extract persons, organisations, and locations from a spaCy Doc."""
    entities: Dict[str, List[str]] = {
        "persons": [],
        "organizations": [],
        "locations": [],
    }
    seen: Dict[str, set] = {k: set() for k in entities}

    for ent in doc.ents:
        text = ent.text.strip()
        if not text or len(text) < 2:
            continue

        if ent.label_ == "PERSON" and text.lower() not in seen["persons"]:
            entities["persons"].append(text)
            seen["persons"].add(text.lower())
        elif ent.label_ == "ORG" and text.lower() not in seen["organizations"]:
            entities["organizations"].append(text)
            seen["organizations"].add(text.lower())
        elif ent.label_ in ("GPE", "LOC") and text.lower() not in seen["locations"]:
            entities["locations"].append(text)
            seen["locations"].add(text.lower())

    return entities


# ---------------------------------------------------------------------------
# Keywords
# ---------------------------------------------------------------------------

def extract_keywords(text: str) -> List[str]:
    """Return the top YAKE keywords for *text*."""
    extractor = _get_yake()
    try:
        kws = extractor.extract_keywords(text)
        return [kw for kw, _score in kws]
    except Exception:
        logger.exception("Keyword extraction failed.")
        return []


# ---------------------------------------------------------------------------
# Sentiment
# ---------------------------------------------------------------------------

def analyze_sentiment(text: str) -> float:
    """Return VADER compound sentiment score in [-1, +1]."""
    analyzer = _get_vader()
    try:
        scores = analyzer.polarity_scores(text)
        return scores["compound"]
    except Exception:
        logger.exception("Sentiment analysis failed.")
        return 0.0


# ---------------------------------------------------------------------------
# Summarisation
# ---------------------------------------------------------------------------

def summarize(text: str, sentence_count: int = 0) -> str:
    """Extractive summary via sumy LexRank.

    Parameters
    ----------
    text:
        The source text to summarise.
    sentence_count:
        Number of sentences.  Defaults to ``config.summary_sentences``.
    """
    if sentence_count <= 0:
        sentence_count = config.summary_sentences

    try:
        from sumy.parsers.plaintext import PlaintextParser
        from sumy.nlp.tokenizers import Tokenizer
        from sumy.summarizers.lex_rank import LexRankSummarizer

        parser = PlaintextParser.from_string(text, Tokenizer("english"))
        summarizer = LexRankSummarizer()
        sentences = summarizer(parser.document, sentence_count)
        return " ".join(str(s) for s in sentences)
    except Exception:
        logger.exception("Summarisation failed -- returning truncated text.")
        return text[:500]


# ---------------------------------------------------------------------------
# Geo-tagging
# ---------------------------------------------------------------------------

def geolocate(entities: Dict[str, List[str]], feed_region: str = "global") -> str:
    """Map extracted GPE entities to a canonical region.

    Heuristic: take the most frequently matched region across all location
    entities.  Fall back to the feed's own region tag.
    """
    region_counts: Dict[str, int] = {}

    for loc in entities.get("locations", []):
        key = loc.lower().strip()
        region = GPE_REGION_MAP.get(key)
        if region:
            region_counts[region] = region_counts.get(region, 0) + 1

    if region_counts:
        best = max(region_counts, key=region_counts.get)  # type: ignore[arg-type]
        return best

    # Fallback to feed-level region
    if feed_region in VALID_REGIONS:
        return feed_region
    return "global"


# ---------------------------------------------------------------------------
# Intensity classification
# ---------------------------------------------------------------------------

def classify_intensity(sentiment: float, text: str) -> str:
    """Classify article intensity.

    Rules:
      - critical:    sentiment < -0.7 AND >= 3 threat keywords
      - high-threat: sentiment < -0.5 AND >= 1 threat keyword
      - moderate:    sentiment < -0.2 OR >= 1 threat keyword
      - low:         everything else
    """
    text_lower = text.lower()
    threat_count = sum(1 for kw in THREAT_KEYWORDS if kw in text_lower)

    if sentiment < -0.7 and threat_count >= 3:
        return "critical"
    if sentiment < -0.5 and threat_count >= 1:
        return "high-threat"
    if sentiment < -0.2 or threat_count >= 1:
        return "moderate"
    return "low"


# ---------------------------------------------------------------------------
# Single-article processing
# ---------------------------------------------------------------------------

def process_article(article: Article, feed_region: str = "global") -> Optional[ArticleBrief]:
    """Run the full NLP pipeline on a single Article.

    Returns an ArticleBrief (not yet committed) or None on failure.
    """
    try:
        # Combine title + description for richer analysis
        raw_text = f"{article.title}. {article.description or ''}"

        # 1. Language detection
        lang = detect_language(raw_text)
        article.original_lang = lang

        # 2. Translation
        english_text = translate_to_english(raw_text, lang)

        # 3. NER with spaCy
        nlp = _get_spacy()
        doc = nlp(english_text[:100_000])  # cap input for safety
        entities = extract_entities(doc)

        # 4. Keywords
        keywords = extract_keywords(english_text)

        # 5. Sentiment
        sentiment = analyze_sentiment(english_text)

        # 6. Summary
        summary = summarize(english_text)

        # 7. Geo-tagging
        region = geolocate(entities, feed_region)

        # 8. Intensity
        intensity = classify_intensity(sentiment, english_text)

        brief = ArticleBrief(
            article_id=article.id,
            region=region,
            entities_json=json.dumps(entities, ensure_ascii=False),
            keywords_json=json.dumps(keywords, ensure_ascii=False),
            sentiment=sentiment,
            intensity=intensity,
            summary=summary,
            processed_at=datetime.utcnow(),
        )
        return brief

    except Exception:
        logger.exception("NLP pipeline failed for article %s", article.id)
        return None


# ---------------------------------------------------------------------------
# Batch processing with BERTopic clustering
# ---------------------------------------------------------------------------

def process_batch(articles: List[Article]) -> List[ArticleBrief]:
    """Process a batch of articles and perform topic clustering.

    Steps:
      1. Run ``process_article`` on each article.
      2. Cluster all resulting briefs with BERTopic.
      3. Assign ``topic_cluster_id`` on each brief.
      4. Persist briefs to the database.

    Returns
    -------
    list[ArticleBrief]
        The persisted ArticleBrief objects.
    """
    if not articles:
        return []

    logger.info("Processing batch of %d articles...", len(articles))

    # Build a region lookup from feed config for fallback geo-tagging
    feed_region_map: Dict[str, str] = {}
    for feed_def in config.feeds:
        feed_region_map[feed_def["name"]] = feed_def.get("region", "global")

    # --- Per-article NLP ---
    briefs: List[ArticleBrief] = []
    texts_for_clustering: List[str] = []

    for article in articles:
        feed_region = feed_region_map.get(article.source, "global")
        brief = process_article(article, feed_region)
        if brief is not None:
            briefs.append(brief)
            texts_for_clustering.append(
                f"{article.title}. {article.description or ''}"
            )

    if not briefs:
        logger.warning("No briefs produced from batch.")
        return []

    # --- BERTopic clustering ---
    if len(texts_for_clustering) >= 5:  # BERTopic needs a minimum corpus
        try:
            topic_model = _get_topic_model()
            if topic_model is not None:
                topics, _probs = topic_model.fit_transform(texts_for_clustering)
                for idx, brief in enumerate(briefs):
                    brief.topic_cluster_id = int(topics[idx]) if topics[idx] != -1 else None
                logger.info(
                    "BERTopic clustering complete: %d unique topics.",
                    len(set(t for t in topics if t != -1)),
                )
        except Exception:
            logger.exception("BERTopic clustering failed -- skipping cluster IDs.")
    else:
        logger.info(
            "Fewer than 5 articles (%d) -- skipping BERTopic clustering.",
            len(texts_for_clustering),
        )

    # --- Persist briefs ---
    session = get_session()
    try:
        for brief in briefs:
            session.add(brief)
        session.commit()
        logger.info("Saved %d article briefs.", len(briefs))
    except Exception:
        logger.exception("Failed to save article briefs -- rolling back.")
        session.rollback()
        return []
    finally:
        session.close()

    return briefs
