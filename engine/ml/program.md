# OpenClaw ML Research Program

Autonomous experiment loop for improving the signal predictor.
Based on Karpathy's [autoresearch](https://github.com/karpathy/autoresearch) methodology.

## Doel

Verbeter de trading signal predictor door iteratief te experimenteren
met features, hyperparameters, preprocessing, en ensemble methoden.

De primaire metric is `sharpe_ratio` (hoger = beter).
Secundaire metrics: `hit_rate`, `brier_score` (lager = beter).

## Setup

```bash
cd /home/opposite/openclaw-news-analyzer
source engine/venv/bin/activate

# Maak een experiment branch
git checkout -b ml-experiments/$(date +%Y%m%d-%H%M)

# Extract features (eenmalig of na data updates)
python -m engine.ml.prepare

# Establish baseline
python -m engine.ml.train > engine/ml/run.log 2>&1
grep "sharpe_ratio:" engine/ml/run.log
# Noteer deze waarde als BASELINE
```

## Experiment Loop

Herhaal oneindig:

### 1. Analyseer
Lees `engine/ml/train.py` en `engine/ml/run.log`.
Bekijk welke features het meest bijdragen (top_features output).
Denk na over verbeteringen:
- Nieuwe feature combinaties (interacties, ratio's)
- Andere hyperparameters (max_depth, learning_rate, n_estimators)
- Feature selectie (verwijder ruis-features)
- Preprocessing (log transform, binning, outlier handling)
- Ensemble (combineer XGBoost + LightGBM)
- Threshold tuning (PRED_THRESHOLD)
- Andere target variabele (profitable_1d vs profitable_7d)

### 2. Wijzig
Pas **ALLEEN** `engine/ml/train.py` aan.
Wijzig **NOOIT** prepare.py, evaluate.py, of inference.py.

### 3. Commit
```bash
git add engine/ml/train.py
git commit -m "experiment: <korte beschrijving>"
```

### 4. Train
```bash
python -m engine.ml.train > engine/ml/run.log 2>&1
```

### 5. Evalueer
```bash
grep "sharpe_ratio:" engine/ml/run.log
```

### 6. Besluit

**Als sharpe_ratio BETER dan baseline/vorige beste:**
```bash
# Log resultaat
echo "$(git rev-parse --short HEAD)\t$(grep 'sharpe_ratio:' engine/ml/run.log | awk '{print $2}')\t$(grep 'hit_rate:' engine/ml/run.log | awk '{print $2}')\t$(grep 'brier_score:' engine/ml/run.log | awk '{print $2}')\tkeep\t<beschrijving>" >> engine/ml/results.tsv
```
Update baseline. Ga naar stap 1.

**Als sharpe_ratio SLECHTER of GELIJK:**
```bash
# Log resultaat als discarded
echo "$(git rev-parse --short HEAD)\t$(grep 'sharpe_ratio:' engine/ml/run.log | awk '{print $2}')\t$(grep 'hit_rate:' engine/ml/run.log | awk '{print $2}')\t$(grep 'brier_score:' engine/ml/run.log | awk '{print $2}')\tdiscard\t<beschrijving>" >> engine/ml/results.tsv

# Reset
git reset --hard HEAD~1
```
Probeer iets anders. Ga naar stap 1.

## Regels

1. Wijzig **ALLEEN** `engine/ml/train.py`
2. Wijzig **NOOIT** `prepare.py`, `evaluate.py`, of `inference.py`
3. Elke training run moet **< 30 seconden** duren
4. Gebruik **ALTIJD** `TimeSeriesSplit` (nooit random split)
5. **Stop NIET** — experimenteer tot je wordt onderbroken
6. Log **ALLE** experimenten naar `engine/ml/results.tsv`
7. Commit **voor** je runt (zodat je kunt resetten bij falen)

## Experiment Ideeën (startpunt)

1. **Feature engineering**: sentiment_mean × runup_score interactie
2. **Log transforms**: np.log1p(article_count_7d)
3. **Temporal features**: dag van de week, uur van detectie
4. **Ensemble**: XGBoost + LightGBM voting
5. **Hyperparameter sweep**: learning_rate van 0.01 tot 0.3
6. **Feature selection**: verwijder features met importance < 0.01
7. **Target experiment**: profitable_1d vs profitable_3d vs profitable_7d
8. **Threshold scan**: PRED_THRESHOLD van 0.4 tot 0.7
9. **Class weighting**: experimenteer met scale_pos_weight overrides
10. **Gradient boosting rounds**: early stopping na N rounds zonder verbetering

## Results Format

`engine/ml/results.tsv` (tab-separated):
```
commit  sharpe  hit_rate  brier  status  description
abc1234  0.5432  0.6500  0.2341  keep  baseline XGBoost
def5678  0.4100  0.5800  0.2890  discard  reduced max_depth to 2
```
