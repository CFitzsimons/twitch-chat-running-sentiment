from flask import Flask, request, jsonify
from transformers import pipeline

# initial load, so we aren't recreating
# on every call
classifier = pipeline(
    task="text-classification",
    model="SamLowe/roberta-base-go_emotions",
    top_k=None
)

app = Flask(__name__)

# TODO: Wire this up to the dockerfile for a healthcheck
@app.route("/health", methods=["GET"])
def health():
    return jsonify(status="ok")

@app.route("/classify", methods=["POST"])
def classify():
    data = request.get_json(force=True)
    texts = data.get("texts")
    if texts is None:
        text = data.get("text")
        if text is None:
            return jsonify(error="Provide 'text' or 'texts'"), 400
        texts = [text]

    outputs = classifier(texts)
    all_results = []
    for per_text in outputs:
        label_scores = {item["label"]: float(item["score"]) for item in per_text}
        all_results.append(label_scores)
    if len(all_results) == 1:
        return jsonify(result=all_results[0])
    else:
        return jsonify(results=all_results)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5500)