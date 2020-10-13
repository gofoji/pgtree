CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
ALTER TYPE mood ADD VALUE 'unknown';
ALTER TYPE mood ADD VALUE 'unknown' AFTER 'ok';
ALTER TYPE mood ADD VALUE 'unknown' BEFORE 'ok';
ALTER TYPE mood ADD VALUE IF NOT EXISTS 'unknown';
ALTER TYPE status RENAME VALUE 'closed' TO 'shut';
