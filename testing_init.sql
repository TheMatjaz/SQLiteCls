BEGIN TRANSACTION;

CREATE TABLE test_points
(
    x REAL NOT NULL,
    y REAL NOT NULL
);

INSERT INTO test_points (x, y)
VALUES (0, 0),
       (1, 1),
       (-1, 0);

COMMIT;
