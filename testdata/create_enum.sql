CREATE TYPE MOOD AS ENUM ('sad', 'ok', 'happy');
ALTER TYPE MOOD ADD VALUE 'unknown';
ALTER TYPE MOOD ADD VALUE 'unknown' after 'ok';
ALTER TYPE MOOD ADD VALUE 'unknown' before 'ok';
ALTER TYPE MOOD ADD VALUE IF NOT EXISTS 'unknown';
ALTER TYPE status RENAME VALUE 'closed' TO 'shut';
