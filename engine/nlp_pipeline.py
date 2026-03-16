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
import re
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

# Urgency keywords for urgency scoring
URGENCY_HIGH = {"breaking", "imminent", "just now", "hours ago", "urgent", "alert", "emergency", "developing"}
URGENCY_ACTION = {"launched", "struck", "invaded", "attacked", "fired", "deployed", "seized", "bombed", "killed"}
URGENCY_PLANNING = {"planned", "discussed", "proposed", "considered", "expected", "likely", "may", "could"}

# Event type classification keywords
EVENT_TYPES = {
    "military_action": {"strike", "attack", "missile", "troops", "deployed", "invasion", "airstrike", "drone", "bombing", "shelling", "military operation"},
    "diplomatic": {"talks", "summit", "agreement", "ceasefire", "negotiate", "treaty", "diplomacy", "mediation", "peace process", "dialogue"},
    "economic": {"sanctions", "tariff", "trade", "embargo", "oil price", "inflation", "currency", "gdp", "recession", "investment"},
    "social": {"protest", "demonstration", "unrest", "refugees", "humanitarian", "displacement", "riot", "civil unrest"},
    "legal": {"indictment", "tribunal", "resolution", "icc", "court", "prosecution", "verdict", "ruling", "investigation"},
}

# Source credibility ratings (same as deep_analysis.py but used for per-article scoring)
SOURCE_CREDIBILITY = {
    "Reuters": 0.95, "AP News": 0.95, "AFP": 0.90,
    "BBC": 0.90, "The Guardian": 0.88, "NPR": 0.88,
    "Financial Times": 0.90, "Bloomberg": 0.88,
    "CNN": 0.78, "CNBC": 0.80, "Fox News": 0.60,
    "Al Jazeera English": 0.82, "Al Jazeera": 0.82,
    "DW (Deutsche Welle)": 0.85, "France24": 0.83,
    "Defense One": 0.82, "The War Zone": 0.80,
    "RT (Russia Today)": 0.30, "TASS": 0.35,
    "PressTV Iran": 0.35, "IRNA": 0.40, "Press TV": 0.35,
    "Xinhua": 0.40, "CGTN": 0.38,
    # X/Twitter OSINT accounts
    "X/Twitter - @OSINTdefender": 0.72,
    "X/Twitter - @sentdefender": 0.70,
    "X/Twitter - @IntelCrab": 0.70,
    "X/Twitter - @Nrg8000": 0.68,
    "X/Twitter - @AuroraIntel": 0.72,
    "X/Twitter - @Faytuks": 0.68,
    "X/Twitter - @AircraftSpots": 0.74,
    "X/Twitter - @RALee85": 0.78,
    "X/Twitter - @oryxspioenkop": 0.80,
    "X/Twitter - @Conflicts": 0.70,
    "X/Twitter - @christaborowski": 0.68,
    "X/Twitter - @ggreenwald": 0.75,
    "X/Twitter - @MaxBlumenthal": 0.65,
    "X/Twitter - @zerohedge": 0.55,
    "X/Twitter - @wikileaks": 0.70,
    "X/Twitter - @mtracey": 0.62,
    "X/Twitter - @BenjaminNorton": 0.60,
    "X/Twitter - @caitoz": 0.55,
    "X/Twitter - @TheGrayzoneNews": 0.60,
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
    threat_count = sum(1 for kw in THREAT_KEYWORDS if re.search(r'\b' + re.escape(kw) + r'\b', text_lower))

    if sentiment < -0.7 and threat_count >= 3:
        return "critical"
    if sentiment < -0.5 and threat_count >= 1:
        return "high-threat"
    if sentiment < -0.2 or threat_count >= 1:
        return "moderate"
    return "low"


# ---------------------------------------------------------------------------
# Urgency scoring
# ---------------------------------------------------------------------------

def score_urgency(text: str, source: str = "") -> float:
    """Score article urgency from 0 (background) to 1 (breaking/imminent).

    Based on:
      - Presence of urgency keywords (breaking, imminent, etc.)
      - Action verbs vs planning verbs
      - Source type (wire services score higher)
    """
    text_lower = text.lower()

    # Urgency keyword count
    urgency_hits = sum(1 for kw in URGENCY_HIGH if re.search(r'\b' + re.escape(kw) + r'\b', text_lower))
    action_hits = sum(1 for kw in URGENCY_ACTION if re.search(r'\b' + re.escape(kw) + r'\b', text_lower))
    planning_hits = sum(1 for kw in URGENCY_PLANNING if re.search(r'\b' + re.escape(kw) + r'\b', text_lower))

    # Base score from keywords
    score = min(urgency_hits * 0.25 + action_hits * 0.15, 0.7)

    # Reduce for planning language
    if planning_hits > action_hits:
        score *= 0.6

    # Boost for wire service sources
    wire_services = {"Reuters", "AP News", "AFP"}
    if source in wire_services:
        score = min(score + 0.15, 1.0)

    return round(min(max(score, 0.0), 1.0), 2)


# ---------------------------------------------------------------------------
# Event type classification
# ---------------------------------------------------------------------------

def classify_event_type(text: str) -> str:
    """Classify article into event type based on keyword matching."""
    text_lower = text.lower()
    type_scores = {}
    for event_type, keywords in EVENT_TYPES.items():
        hits = sum(1 for kw in keywords if re.search(r'\b' + re.escape(kw) + r'\b', text_lower))
        if hits > 0:
            type_scores[event_type] = hits

    if not type_scores:
        return "general"
    return max(type_scores, key=type_scores.get)


# ---------------------------------------------------------------------------
# Key actor extraction
# ---------------------------------------------------------------------------

def extract_key_actors(doc) -> list:
    """Extract key actors (persons/orgs performing actions) from spaCy doc."""
    actors = []
    seen = set()

    for ent in doc.ents:
        if ent.label_ not in ("PERSON", "ORG"):
            continue
        name = ent.text.strip()
        if not name or len(name) < 2 or name.lower() in seen:
            continue

        # Check if entity is subject of an action verb
        action = None
        for tok in ent:
            if tok.dep_ in ("nsubj", "nsubjpass") and tok.head.pos_ == "VERB":
                action = tok.head.lemma_
                break

        if action:
            actors.append({
                "name": name,
                "type": ent.label_.lower(),
                "action": action,
            })
            seen.add(name.lower())

    return actors[:10]  # Limit to top 10 actors


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
    """Process a batch of articles with optimised spaCy batching.

    Uses nlp.pipe() for 3-10x faster NER processing and ThreadPoolExecutor
    for parallel language detection + translation.

    Steps:
      1. Pre-process: language detection + translation (parallel).
      2. Batch NER with spaCy nlp.pipe().
      3. Per-doc enrichment: keywords, sentiment, summary, geo, intensity,
         urgency, event type, key actors, source credibility.
      4. BERTopic clustering (if enough articles).
      5. Persist briefs to database.

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

    # --- Phase 1: Pre-process (language detection + translation) ---
    # These are I/O-light CPU tasks that can run in threads
    from concurrent.futures import ThreadPoolExecutor

    def _prepare(article):
        raw_text = f"{article.title}. {article.description or ''}"
        lang = detect_language(raw_text)
        english_text = translate_to_english(raw_text, lang)
        return {
            "text": english_text[:100_000],
            "lang": lang,
            "feed_region": feed_region_map.get(article.source, "global"),
        }

    with ThreadPoolExecutor(max_workers=4) as pool:
        prep_results = list(pool.map(_prepare, articles))

    # --- Phase 2: Batch NER with spaCy nlp.pipe() (3-10x faster) ---
    nlp = _get_spacy()
    english_texts = [r["text"] for r in prep_results]
    docs = list(nlp.pipe(english_texts, batch_size=32))

    # --- Phase 3: Per-doc enrichment ---
    briefs: List[ArticleBrief] = []
    texts_for_clustering: List[str] = []

    for article, prep, doc in zip(articles, prep_results, docs):
        try:
            english_text = prep["text"]
            feed_region = prep["feed_region"]

            # Set original language on article
            article.original_lang = prep["lang"]

            # NER
            entities = extract_entities(doc)

            # Keywords
            keywords = extract_keywords(english_text)

            # Sentiment
            sentiment = analyze_sentiment(english_text)

            # Summary
            summary = summarize(english_text)

            # Geo-tagging
            region = geolocate(entities, feed_region)

            # Intensity
            intensity = classify_intensity(sentiment, english_text)

            # New analysis points (v2)
            urgency = score_urgency(english_text, article.source)
            credibility = SOURCE_CREDIBILITY.get(article.source, 0.60)
            event_type = classify_event_type(english_text)
            actors = extract_key_actors(doc)

            brief = ArticleBrief(
                article_id=article.id,
                region=region,
                entities_json=json.dumps(entities, ensure_ascii=False),
                keywords_json=json.dumps(keywords, ensure_ascii=False),
                sentiment=sentiment,
                intensity=intensity,
                summary=summary,
                processed_at=datetime.utcnow(),
                # v2 fields
                urgency_score=urgency,
                source_credibility=credibility,
                key_actors_json=json.dumps(actors, ensure_ascii=False),
                event_type=event_type,
            )
            briefs.append(brief)
            texts_for_clustering.append(f"{article.title}. {article.description or ''}")

        except Exception:
            logger.exception("NLP pipeline failed for article %s", article.id)
            continue

    if not briefs:
        logger.warning("No briefs produced from batch.")
        return []

    # --- Phase 4: BERTopic clustering ---
    if len(texts_for_clustering) >= 5:  # BERTopic needs a minimum corpus
        try:
            topic_model = _get_topic_model()
            if topic_model is not None:
                try:
                    # Try to use existing fitted model for consistent cluster IDs
                    topics, _probs = topic_model.transform(texts_for_clustering)
                except Exception:
                    # First batch or model not fitted yet — do initial fit
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

    # --- Phase 5: Persist briefs ---
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
