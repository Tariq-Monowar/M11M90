-- Fix username column: rename from "userName" to username (lowercase, no quotes)
-- First, add the new column
ALTER TABLE users ADD COLUMN IF NOT EXISTS username VARCHAR(255);

-- Copy data from old column to new column
UPDATE users SET username = "userName" WHERE username IS NULL;

-- Make username NOT NULL
ALTER TABLE users ALTER COLUMN username SET NOT NULL;

-- Drop the old column
ALTER TABLE users DROP COLUMN IF EXISTS "userName";
