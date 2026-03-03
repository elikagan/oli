-- OLI Schema — run this in Supabase SQL Editor

-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Listings table
CREATE TABLE listings (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  platform TEXT NOT NULL,
  platform_id TEXT NOT NULL,
  title TEXT,
  description TEXT,
  price NUMERIC,
  currency TEXT DEFAULT 'USD',
  location TEXT,
  url TEXT NOT NULL,
  image_urls TEXT[],
  hero_image TEXT,
  auction_house TEXT,
  auction_date TIMESTAMPTZ,
  lot_number TEXT,
  ai_description TEXT,
  embedding VECTOR(768),
  status TEXT DEFAULT 'active',
  scraped_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(platform, platform_id)
);

CREATE INDEX idx_listings_platform ON listings(platform);
CREATE INDEX idx_listings_status ON listings(status);

-- Swipes table
CREATE TABLE swipes (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  listing_id UUID REFERENCES listings(id) ON DELETE CASCADE,
  action TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_swipes_action ON swipes(action);
CREATE INDEX idx_swipes_listing ON swipes(listing_id);

-- Favorites table
CREATE TABLE favorites (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  listing_id UUID REFERENCES listings(id) ON DELETE CASCADE UNIQUE,
  notes TEXT,
  status TEXT DEFAULT 'new',
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Taste profile (singleton)
CREATE TABLE taste_profile (
  id INTEGER PRIMARY KEY DEFAULT 1,
  positive_centroid VECTOR(768),
  negative_centroid VECTOR(768),
  positive_count INTEGER DEFAULT 0,
  negative_count INTEGER DEFAULT 0,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert empty taste profile row
INSERT INTO taste_profile (id, positive_count, negative_count) VALUES (1, 0, 0);

-- RPC: match listings by embedding similarity
CREATE OR REPLACE FUNCTION match_listings(
  query_embedding VECTOR(768),
  match_count INT DEFAULT 20,
  exclude_ids UUID[] DEFAULT '{}'
)
RETURNS TABLE (
  id UUID,
  platform TEXT,
  platform_id TEXT,
  title TEXT,
  description TEXT,
  price NUMERIC,
  location TEXT,
  url TEXT,
  hero_image TEXT,
  image_urls TEXT[],
  auction_house TEXT,
  auction_date TIMESTAMPTZ,
  lot_number TEXT,
  ai_description TEXT,
  similarity FLOAT
)
LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  SELECT
    sl.id, sl.platform, sl.platform_id, sl.title, sl.description,
    sl.price, sl.location, sl.url, sl.hero_image, sl.image_urls,
    sl.auction_house, sl.auction_date, sl.lot_number, sl.ai_description,
    (1 - (sl.embedding <=> query_embedding))::FLOAT AS similarity
  FROM listings sl
  WHERE sl.status = 'active'
    AND sl.embedding IS NOT NULL
    AND sl.id != ALL(exclude_ids)
  ORDER BY sl.embedding <=> query_embedding
  LIMIT match_count;
END;
$$;

-- RLS: anon can read listings and favorites, service key can write everything
ALTER TABLE listings ENABLE ROW LEVEL SECURITY;
ALTER TABLE swipes ENABLE ROW LEVEL SECURITY;
ALTER TABLE favorites ENABLE ROW LEVEL SECURITY;
ALTER TABLE taste_profile ENABLE ROW LEVEL SECURITY;

-- Anon read access
CREATE POLICY "anon_read_listings" ON listings FOR SELECT USING (true);
CREATE POLICY "anon_read_favorites" ON favorites FOR SELECT USING (true);
CREATE POLICY "anon_read_taste" ON taste_profile FOR SELECT USING (true);

-- Service role full access (worker uses service key)
CREATE POLICY "service_all_listings" ON listings FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "service_all_swipes" ON swipes FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "service_all_favorites" ON favorites FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "service_all_taste" ON taste_profile FOR ALL USING (auth.role() = 'service_role');
