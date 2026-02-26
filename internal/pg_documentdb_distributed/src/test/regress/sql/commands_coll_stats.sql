SET search_path TO documentdb_api,documentdb_core;
SET citus.next_shard_id TO 990000;
SET documentdb.next_collection_id TO 9900;
SET documentdb.next_collection_index_id TO 9900;

-- Setup: Create collections with different data sizes
SELECT COUNT(*) FROM (SELECT documentdb_api.insert_one('coll_stats_test_db', 'test_coll1', FORMAT('{ "_id": %s, "a": %s, "b": "test_string_%s" }', i, i, i)::documentdb_core.bson) FROM generate_series(1, 1000) i) innerQuery;
SELECT COUNT(*) FROM (SELECT documentdb_api.insert_one('coll_stats_test_db', 'test_coll2', FORMAT('{ "_id": %s, "x": %s, "y": %s }', i, i, i * 2)::documentdb_core.bson) FROM generate_series(1, 500) i) innerQuery;
SELECT COUNT(*) FROM (SELECT documentdb_api.insert_one('coll_stats_test_db', 'test_coll3', FORMAT('{ "_id": %s, "data": %s }', i, i)::documentdb_core.bson) FROM generate_series(1, 100) i) innerQuery;

-- Create indexes on test collections
SELECT documentdb_api_internal.create_indexes_non_concurrently('coll_stats_test_db', '{ "createIndexes": "test_coll1", "indexes": [ { "key": { "a": 1 }, "name": "a_1" }]}', TRUE);
SELECT documentdb_api_internal.create_indexes_non_concurrently('coll_stats_test_db', '{ "createIndexes": "test_coll2", "indexes": [ { "key": { "x": 1 }, "name": "x_1" }, { "key": { "y": 1 }, "name": "y_1" }]}', TRUE);

-- Analyze for deterministic stats
ANALYZE;

-- Test 1: Basic coll_stats with BSON spec (collStats command format)
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll1", "$db": "coll_stats_test_db" }'::documentdb_core.bson);

-- Test 2: coll_stats with scale parameter in BSON spec
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll1", "$db": "coll_stats_test_db", "scale": 1 }'::documentdb_core.bson);
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll1", "$db": "coll_stats_test_db", "scale": 1024 }'::documentdb_core.bson);

-- Test 3: coll_stats for collection with multiple indexes
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll2", "$db": "coll_stats_test_db" }'::documentdb_core.bson);

-- Test 4: coll_stats for smaller collection
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll3", "$db": "coll_stats_test_db" }'::documentdb_core.bson);

-- Test 5: coll_stats with different scale values
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll1", "$db": "coll_stats_test_db", "scale": 2 }'::documentdb_core.bson);
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll1", "$db": "coll_stats_test_db", "scale": 100 }'::documentdb_core.bson);
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll1", "$db": "coll_stats_test_db", "scale": 2147483647 }'::documentdb_core.bson);

-- Test 6: coll_stats for non-existent collection (error case)
SELECT documentdb_api.coll_stats('{ "collStats": "non_existent_coll", "$db": "coll_stats_test_db" }'::documentdb_core.bson);

-- Test 7: coll_stats for non-existent database (error case)
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll1", "$db": "non_existent_db" }'::documentdb_core.bson);

-- Test 8: coll_stats with invalid scale (error cases)
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll1", "$db": "coll_stats_test_db", "scale": 0 }'::documentdb_core.bson);
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll1", "$db": "coll_stats_test_db", "scale": -1 }'::documentdb_core.bson);
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll1", "$db": "coll_stats_test_db", "scale": -100 }'::documentdb_core.bson);

-- Test 9: coll_stats with missing required fields (error cases)
SELECT documentdb_api.coll_stats('{ "$db": "coll_stats_test_db" }'::documentdb_core.bson);
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll1" }'::documentdb_core.bson);

-- Test 10: coll_stats with fractional scale values
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll1", "$db": "coll_stats_test_db", "scale": 1.5 }'::documentdb_core.bson);
SELECT documentdb_api.coll_stats('{ "collStats": "test_coll1", "$db": "coll_stats_test_db", "scale": 1024.99 }'::documentdb_core.bson);

-- Cleanup
SET client_min_messages TO WARNING;
SELECT documentdb_api.drop_database('coll_stats_test_db');
SET client_min_messages TO DEFAULT;