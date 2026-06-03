# ============================================================
# PDF ML training script: detect TSR vs non-TSR N-CSR filings
#
# Note: This is Python code, not R code.
# It is uploaded to GitHub only for information and transparency about how the TSR PDF classifier was trained.
#
# What this script does:
#   - Reads labeled TSR and non-TSR PDF filings.
#   - Extracts readable text from each PDF file.
#   - Focuses mainly on the Item/Section 1 area of the filing.
#   - Trains a TF-IDF + Logistic Regression classifier.
#   - Saves the trained model and review files for checking predictions.
#
# Labels:
#   y = 1 : TSR PDF files
#   y = 0 : Non-TSR PDF files
#
# Main outputs:
#   - tsr_pdf_classifier.joblib
#   - training data snapshot CSV
#   - prediction/review CSV
# ============================================================

from pathlib import Path
import pandas as pd
import numpy as np
import re
from datetime import datetime

from pypdf import PdfReader
from sklearn.model_selection import train_test_split
from sklearn.pipeline import FeatureUnion, Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
import joblib
from tqdm import tqdm

# -----------------------
# Section/Item 1 text window
# -----------------------
# TSR-related disclosure content is usually near the beginning of the N-CSR.
# This helper tries to isolate the Item/Section 1 part of the filing.

START_PAT = re.compile(
  r"\b(?:item|section)\s*1\s*(?:a)?\b",
  flags=re.I
)

END_PAT = re.compile(
  r"\b(?:item|section)\s*2\b",
  flags=re.I
)

def extract_section1_window(text: str, window_chars: int = 60_000) -> str:
  """
    Extract the part of the filing most likely to contain Item/Section 1.

    The function first looks for Item/Section 1. If Item/Section 2 appears
    soon after that, it keeps the text between them. If not, it keeps a fixed
    window after Item/Section 1. If Item/Section 1 is not found, it falls back
    to the beginning of the document.
    """
if not text:
  return ""

m = START_PAT.search(text)
if not m:
  return text[:window_chars]

start = m.start()

m2 = END_PAT.search(text, pos=start + 200)
if m2 and (m2.start() - start) <= (window_chars * 2):
  end = m2.start()
return text[start:end][:window_chars]

return text[start:start + window_chars]

# -----------------------
# 0) Paths
# -----------------------
# Main folder containing the labeled PDF training folders and model outputs.
BASE = Path("/root/tsr_project/NCSR Reports")

# Labeled training folders.
FOLDER_TSR_PDF     = BASE / "Post_TSR_PDF"   # label = 1
FOLDER_NON_TSR_PDF = BASE / "Pre_TSR_PDF"    # label = 0

# Folder where the trained model and review files will be saved.
OUT_DIR = BASE / "model_training_outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_MODEL = OUT_DIR / "tsr_pdf_classifier.joblib"
stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_DATA  = OUT_DIR / f"tsr_pdf_training_data_{stamp}.csv"
OUT_PRED  = OUT_DIR / f"tsr_pdf_predictions_{stamp}.csv"

# -----------------------
# 1) Text extraction and cleaning
# -----------------------
# Keep enough text to capture the useful filing content, but cap the length so
# model training stays manageable.
MAX_CHARS = 800_000

def normalize_text(s: str) -> str:
  if not s:
  return ""
s = s.replace("\x00", " ").replace("\u00A0", " ")
s = s.replace("\u2010", "-").replace("\u2011", "-").replace("\u2012", "-")
s = s.replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-")
s = re.sub(r"\s+", " ", s)
return s.strip()

def pdf_to_text(path: Path) -> str:
  try:
  reader = PdfReader(str(path))
except Exception:
  return ""

pages = []
for page in reader.pages:
  try:
  txt = page.extract_text() or ""
except Exception:
  txt = ""
pages.append(txt)

txt = " ".join(pages)
txt = normalize_text(txt)
return txt[:MAX_CHARS]

def list_pdf(folder: Path):
  if not folder.exists():
  raise FileNotFoundError(f"Missing folder: {folder}")
return sorted([p for p in folder.rglob("*") if p.is_file() and p.suffix.lower() == ".pdf"])

# -----------------------
# 2) Build the labeled training dataset
# -----------------------
# The positive folder contains TSR examples, and the negative folder contains
# traditional non-TSR N-CSR examples.
pos_files = list_pdf(FOLDER_TSR_PDF)
neg_files = list_pdf(FOLDER_NON_TSR_PDF)

print("TSR PDF files:", len(pos_files))
print("Non-TSR PDF files:", len(neg_files))

if len(pos_files) == 0:
  raise ValueError(f"No positive PDF files found in: {FOLDER_TSR_PDF}")
if len(neg_files) == 0:
  raise ValueError(f"No negative PDF files found in: {FOLDER_NON_TSR_PDF}")

rows = []
for p in pos_files:
  rows.append({"path": str(p), "label": 1})
for p in neg_files:
  rows.append({"path": str(p), "label": 0})

df = pd.DataFrame(rows)
df["filename"] = df["path"].apply(lambda x: Path(x).name)

texts = []
low_text = 0

for r in tqdm(df.itertuples(index=False), total=len(df), desc="Extracting PDF text"):
  t = pdf_to_text(Path(r.path))
if len(t) < 300:
  low_text += 1
texts.append(t)

df["text_full"] = texts
df["text"] = df["text_full"].apply(lambda t: extract_section1_window(t, window_chars=60_000))

if len(df) > 0:
  print(df["text"].iloc[0][:500])

print("Very low text docs (<300 chars):", low_text)

# This diagnostic flag checks whether the extracted text still contains an
# Item/Section 1 marker.
ITEM1_PATTERN = re.compile(r"\b(item|section)\s*1\s*(a|A)?\b", flags=re.I)
df["has_item1"] = df["text"].apply(lambda t: int(bool(ITEM1_PATTERN.search(t))))

df.to_csv(OUT_DATA, index=False)
print("Saved training snapshot:", OUT_DATA)

# -----------------------
# 3) Train/test split
# -----------------------
# Stratification keeps the TSR/non-TSR balance similar in both train and test sets.
X_text = df["text"].values
y = df["label"].values

X_train, X_test, y_train, y_test, df_train, df_test = train_test_split(
  X_text, y, df, test_size=0.25, random_state=42, stratify=y
)

# -----------------------
# 4) Model specification
# -----------------------
# Word n-grams capture meaningful phrases.
# Character n-grams help with SEC formatting, spelling variations, and messy PDF text.
word_tfidf = TfidfVectorizer(
  lowercase=True,
  stop_words="english",
  ngram_range=(1, 2),
  min_df=2,
  max_features=250_000
)

char_tfidf = TfidfVectorizer(
  lowercase=True,
  analyzer="char_wb",
  ngram_range=(3, 5),
  min_df=2,
  max_features=250_000
)

features = FeatureUnion([
  ("word", word_tfidf),
  ("char", char_tfidf),
])

clf = LogisticRegression(
  max_iter=4000,
  class_weight="balanced"
)

model = Pipeline([
  ("features", features),
  ("clf", clf)
])

model.fit(X_train, y_train)

# -----------------------
# 5) Evaluate the classifier
# -----------------------
# The probability cutoff is 0.50 here because the model is being reviewed
# as a standard binary classifier.
proba = model.predict_proba(X_test)[:, 1]
pred  = (proba >= 0.5).astype(int)

print("\n=== Confusion Matrix (rows=true, cols=pred) ===")
print(confusion_matrix(y_test, pred))

print("\n=== Classification Report ===")
print(classification_report(y_test, pred, digits=4))

try:
  print("ROC AUC:", round(roc_auc_score(y_test, proba), 4))
except Exception:
  pass

# -----------------------
# 6) Save the trained model
# -----------------------
# This .joblib file can later be used by a TSR detector script.
joblib.dump(model, OUT_MODEL)
print("Saved model:", OUT_MODEL)

# -----------------------
# 7) Predict on all labeled files for manual review
# -----------------------
# This output helps check whether any training examples look suspicious or mislabeled.
df["p_tsr"] = model.predict_proba(df["text"].values)[:, 1]
df["pred"]  = (df["p_tsr"] >= 0.5).astype(int)

df_out = df[["filename", "label", "pred", "p_tsr", "has_item1", "path"]].sort_values(
  "p_tsr", ascending=False
)
df_out.to_csv(OUT_PRED, index=False)
print("Saved predictions CSV:", OUT_PRED)