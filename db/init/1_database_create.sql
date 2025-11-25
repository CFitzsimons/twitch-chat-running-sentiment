CREATE TABLE
  IF NOT EXISTS message_sentiment (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT Now (),
    channel TEXT NOT NULL,
    original_message TEXT NOT NULL,
    cleaned_message TEXT NOT NULL,
    top_label TEXT NOT NULL,
    top_score DOUBLE PRECISION NOT NULL,
    scores jsonb NOT NULL
  );

CREATE INDEX IF NOT EXISTS idx_message_sentiment_created_at ON message_sentiment (created_at);

CREATE INDEX IF NOT EXISTS idx_message_sentiment_top_label ON message_sentiment (top_label);

CREATE INDEX IF NOT EXISTS idx_message_sentiment_channel ON message_sentiment (channel);