-- upgrade --
CREATE TABLE IF NOT EXISTS "friends" (
    "id" INTEGER PRIMARY KEY NOT NULL,
    "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "modified_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "idnumber" VARCHAR(256) NOT NULL UNIQUE
    "name" VARCHAR(256) NOT NULL,
    "surname" VARCHAR(256) NOT NULL,
    "level" INTEGER NOT NULL DEFAULT 0,
    "dob" DATE NOT NULL,
    "result" DOUBLE PRECISION,
    "comment" VARCHAR(1024)
    "user_id" BIGINT NOT NULL REFERENCES "users" ("id") ON DELETE CASCADE,
    CONSTRAINT "uid_friends_cpkey" UNIQUE ("user_id", "title")
) /* The Notes model */;
