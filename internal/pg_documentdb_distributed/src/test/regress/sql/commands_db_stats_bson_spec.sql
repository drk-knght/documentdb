-- Simple Tests for BSON-based db_stats UDF (documentdb_api.db_stats)
-- This test is based on the new signature used by the Gateway (RFC: 0010)

SET search_path TO documentdb_core,documentdb_api,documentdb_api_catalog,documentdb_api_internal,documentdb_api;
SET citus.next_shard_id TO 2950000;
SET documentdb.next_collection_id TO 29500;
SET documentdb.next_collection_index_id TO 29500;

-- Utility function to add multiple documents to a collection.
CREATE OR REPLACE FUNCTION insert_docs(p_db TEXT, p_coll TEXT, p_num INT, p_start INT default 0)
RETURNS void
AS $$
DECLARE
    num INTEGER := p_start;
    docText bson;
BEGIN
    WHILE num < p_num + p_start LOOP
        docText :=  CONCAT('{ "a" : ', num, '}');
        PERFORM documentdb_api.insert_one(p_db, p_coll, docText::documentdb_core.bson, NULL);
        num := num + 1;
    END LOOP;
END;
$$
LANGUAGE plpgsql;


SELECT documentdb_api.drop_database('db_bson_test1');

--=============== Tests for Non-existing database should return zero values ===============--

-- Test with only $db field
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Test with $db and scale
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1", "scale": 1}'::bson);
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1", "scale": 1024}'::bson);

--=============== Tests for "collections" & "objects" count ===============+=--

-- Create a Collection
SELECT documentdb_api.create_collection('db_bson_test1', 'col1');

-- db_stats with one empty collection
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1", "scale": 1}'::bson);
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1", "scale": 1024}'::bson);

-- Add one doc
SELECT documentdb_api.insert_one('db_bson_test1','col1',' { "a" : 100 }', NULL);

-- The AutoVacuum might still be napping so count in stats might still be 0,
-- In this test we cannot wait till nap time is over, so we manually trigger the ANALYZE
ANALYZE;

-- db_stats with single collection and single document
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Insert few docs in the collection
SELECT insert_docs('db_bson_test1', 'col1', 20, 1);

-- In this test we cannot wait till Autovaccum nap time is over, so we manually trigger the ANALYZE
ANALYZE;

-- "objects" should be 21, "collections" should be 1
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Create 4 more Collections
SELECT documentdb_api.create_collection('db_bson_test1', 'col2');
SELECT documentdb_api.create_collection('db_bson_test1', 'col3');
SELECT documentdb_api.create_collection('db_bson_test1', 'col4');
SELECT documentdb_api.create_collection('db_bson_test1', 'col5');

-- "collections" and "indexes" count should increase to 5
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Add one doc to each new collection
SELECT documentdb_api.insert_one('db_bson_test1','col2',' { "a" : 100 }', NULL);
SELECT documentdb_api.insert_one('db_bson_test1','col3',' { "a" : 100 }', NULL);
SELECT documentdb_api.insert_one('db_bson_test1','col4',' { "a" : 100 }', NULL);
SELECT documentdb_api.insert_one('db_bson_test1','col5',' { "a" : 100 }', NULL);

-- In this test we cannot wait till Autovaccum nap time is over, so we manually trigger the ANALYZE
ANALYZE;

-- "objects" count should increase to 25
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Insert 20 more docs in each new collection
SELECT insert_docs('db_bson_test1', 'col2', 20, 1);
SELECT insert_docs('db_bson_test1', 'col3', 20, 1);
SELECT insert_docs('db_bson_test1', 'col4', 20, 1);
SELECT insert_docs('db_bson_test1', 'col5', 20, 1);

-- In this test we cannot wait till Autovaccum nap time is over, so we manually trigger the ANALYZE
ANALYZE;

-- "objects" count should increase to 105
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Delete 1 document
SELECT documentdb_api.delete('db_bson_test1', '{"delete":"col1", "deletes":[{"q":{"a":{"$gte": 100}},"limit":0}]}');

-- In this test we cannot wait till Autovaccum nap time is over, so we manually trigger the ANALYZE
ANALYZE;

-- "objects" count should reduce to 104
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Delete 1 document from each remaining collections
SELECT documentdb_api.delete('db_bson_test1', '{"delete":"col2", "deletes":[{"q":{"a":{"$gte": 100}},"limit":0}]}');
SELECT documentdb_api.delete('db_bson_test1', '{"delete":"col3", "deletes":[{"q":{"a":{"$gte": 100}},"limit":0}]}');
SELECT documentdb_api.delete('db_bson_test1', '{"delete":"col4", "deletes":[{"q":{"a":{"$gte": 100}},"limit":0}]}');
SELECT documentdb_api.delete('db_bson_test1', '{"delete":"col5", "deletes":[{"q":{"a":{"$gte": 100}},"limit":0}]}');

-- In this test we cannot wait till Autovaccum nap time is over, so we manually trigger the ANALYZE
ANALYZE;

-- "objects" count should reduce to 100
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Now shard all collections
SELECT documentdb_api.shard_collection('db_bson_test1','col1', '{"a":"hashed"}', false);
SELECT documentdb_api.shard_collection('db_bson_test1','col2', '{"a":"hashed"}', false);
SELECT documentdb_api.shard_collection('db_bson_test1','col3', '{"a":"hashed"}', false);
SELECT documentdb_api.shard_collection('db_bson_test1','col4', '{"a":"hashed"}', false);
SELECT documentdb_api.shard_collection('db_bson_test1','col5', '{"a":"hashed"}', false);

-- "objects" count should remain 100
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);


--===================== Tests for "indexes", "indexSize" =====================--

-- Create one more index
SELECT documentdb_api_internal.create_indexes_non_concurrently('db_bson_test1', documentdb_distributed_test_helpers.generate_create_index_arg('col1', 'index_a_1', '{"a": 1}'), true);

-- "indexes" count should increase to 6, "indexSize" should increase
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Create one more index in each remaining collections
SELECT documentdb_api_internal.create_indexes_non_concurrently('db_bson_test1', documentdb_distributed_test_helpers.generate_create_index_arg('col2', 'index_a_1', '{"a": 1}'), true);
SELECT documentdb_api_internal.create_indexes_non_concurrently('db_bson_test1', documentdb_distributed_test_helpers.generate_create_index_arg('col3', 'index_a_1', '{"a": 1}'), true);
SELECT documentdb_api_internal.create_indexes_non_concurrently('db_bson_test1', documentdb_distributed_test_helpers.generate_create_index_arg('col4', 'index_a_1', '{"a": 1}'), true);
SELECT documentdb_api_internal.create_indexes_non_concurrently('db_bson_test1', documentdb_distributed_test_helpers.generate_create_index_arg('col5', 'index_a_1', '{"a": 1}'), true);

-- "indexes" count should increase to 10, "indexSize" should increase
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Drop one index
CALL documentdb_api.drop_indexes('db_bson_test1', '{"dropIndexes": "col1", "index": "index_a_1"}');

-- "indexes" count should reduce to 9
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Drop one index from each remaining collections
CALL documentdb_api.drop_indexes('db_bson_test1', '{"dropIndexes": "col2", "index": "index_a_1"}');
CALL documentdb_api.drop_indexes('db_bson_test1', '{"dropIndexes": "col3", "index": "index_a_1"}');
CALL documentdb_api.drop_indexes('db_bson_test1', '{"dropIndexes": "col4", "index": "index_a_1"}');
CALL documentdb_api.drop_indexes('db_bson_test1', '{"dropIndexes": "col5", "index": "index_a_1"}');

-- "indexes" count should be back to 5 (one default _id index in each collection), "indexSize" should decrease
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);


--===================== Tests with Views =====================================--

-- create a view on a collection
SELECT documentdb_api.create_collection_view('db_bson_test1', '{ "create": "col1_view1", "viewOn": "col1" }');

-- "views" should be 1
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- create one view on each remaining collection
SELECT documentdb_api.create_collection_view('db_bson_test1', '{ "create": "col2_view1", "viewOn": "col2" }');
SELECT documentdb_api.create_collection_view('db_bson_test1', '{ "create": "col3_view1", "viewOn": "col3" }');
SELECT documentdb_api.create_collection_view('db_bson_test1', '{ "create": "col4_view1", "viewOn": "col4" }');
SELECT documentdb_api.create_collection_view('db_bson_test1', '{ "create": "col5_view1", "viewOn": "col5" }');

-- "views" should be 5
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Drop one collection (despite a view on it)
SELECT documentdb_api.drop_collection('db_bson_test1', 'col5');

-- In this test we cannot wait till Autovaccum nap time is over, so we manually trigger the ANALYZE
ANALYZE;

-- "collections" should be 4, and "objects" will reduce
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Drop one view
SELECT documentdb_api.drop_collection('db_bson_test1', 'col5_view1');

-- "views" should be 4
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Drop all remaining collections
SELECT documentdb_api.drop_collection('db_bson_test1', 'col1');
SELECT documentdb_api.drop_collection('db_bson_test1', 'col2');
SELECT documentdb_api.drop_collection('db_bson_test1', 'col3');
SELECT documentdb_api.drop_collection('db_bson_test1', 'col4');

-- Only "views" and fs stats should be available, rest all should be zero values.
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

--===================== Tests with another database =============================--

-- Make sure this new database does not exist
SELECT documentdb_api.drop_database('db_bson_test2');

-- Add one document
SELECT documentdb_api.insert_one('db_bson_test2','col1',' { "a" : 100 }', NULL);

-- In this test we cannot wait till Autovaccum nap time is over, so we manually trigger the ANALYZE
ANALYZE;

-- various stats should be available
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2"}'::bson);

--===================== Test for "scale" Values =============================--

SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2", "scale": 1}'::bson);
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2", "scale": 2}'::bson);
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2", "scale": 2.5}'::bson);
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2", "scale": 2.99}'::bson);
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2", "scale": 100}'::bson);
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2", "scale": 1024.99}'::bson);
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2", "scale": 2147483647}'::bson);      -- INT_MAX
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2", "scale": 2147483647000}'::bson);   -- More than INT_MAX


--===================== ERROR Cases =============================--

SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2", "scale": 0}'::bson);
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2", "scale": 0.99}'::bson);
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2", "scale": -0.2}'::bson);
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2", "scale": -2}'::bson);
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2", "scale": -2147483648}'::bson);      -- INT_MIN
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2", "scale": -2147483647000}'::bson);   -- Less than INT_MIN


--======================== Clean Up =============================--

SET client_min_messages TO WARNING;

-- Clean up
SELECT documentdb_api.drop_database('db_bson_test1');

-- Should return Zero values for non-existing collection (except of fs stats)
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test1"}'::bson);

-- Clean up
SELECT documentdb_api.drop_database('db_bson_test2');

-- Should return Zero values for non-existing collection (except of fs stats)
SELECT documentdb_api.db_stats('{"dbStats": 1, "$db": "db_bson_test2"}'::bson);

SET client_min_messages TO DEFAULT;
