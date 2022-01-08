from bson.json_util import dumps as bdumps
from pobj import pobjl
import pymongo

logger = pobjl('DevTest', 'debug')


def create_db_conn(database: str):
    """
    - Create MongoDB Connection
    :param database: str: employees_db
    :return:
    """
    db_conn = None
    try:
        # logger.info(f'---create_db_conn--START---')
        logger.info(f'---database: {database}')
        db_client = pymongo.MongoClient("mongodb://localhost:27017/")

        # -> Check if DB is Exists
        logger.debug(f'---db-is-exist: {any([db_name == database for db_name in db_client.list_database_names()])}')

        db_conn = db_client[database]
    except Exception as ex:
        logger.critical('---create_db_conn--funn---EXCEPTION---', exc_info=True)
        logger.critical(f'---create_db_conn--funn---EXCEPTION--msg: {ex}')
    finally:
        logger.info(f'---db_conn: {db_conn}')
        return db_conn


def create_db_collection(db_conn, collection_name: str):
    """
    - Create MongoDB Collection
    :param db_conn:
    :param collection_name: str: personal_details
    :return:
    """
    db_collection = None
    try:
        # logger.info(f'---create_db_collection--START---')
        logger.info(f'---collection_name: {collection_name}')

        # -> Check if Collection is Exists
        logger.debug(f'---collection-is-exist: {any([db_colln == collection_name for db_colln in db_conn.list_collection_names()])}')

        db_collection = db_conn[collection_name]
    except Exception as ex:
        logger.critical('---create_db_collection--funn---EXCEPTION---', exc_info=True)
        logger.critical(f'---create_db_collection--funn---EXCEPTION--msg: {ex}')
    finally:
        logger.info(f'---db_collection: {db_collection}')
        return db_collection


def create_records(db_colln, paramset: dict, insert_multiple=False):
    """
    - Create/Insert Record into MongoDB
    :param db_colln:
    :param paramset: dict: { 'first_name': 'Ashish', 'last_name': 'Soni', 'gender': 'mail', 'age': 23, 'is_active': True }
    :param insert_multiple: boolean: False
    :return:
    """
    last_insert_ids = None
    try:
        # logger.info(f'---create_records--START---')
        logger.info(f'---paramset: {paramset}')
        if insert_multiple:
            result = db_colln.insert_many(paramset)
            last_insert_ids = result.inserted_ids
        else:
            result = db_colln.insert_one(paramset)
            last_insert_ids = result.inserted_id
    except Exception as ex:
        logger.critical('---create_records--funn---EXCEPTION---', exc_info=True)
        logger.critical(f'---create_records--funn---EXCEPTION--msg: {ex}')
    finally:
        logger.info(f'---last_insert_ids: {last_insert_ids}')
        return last_insert_ids


def read_records(db_colln, qry_type, search_paramset: dict = {}, payload: dict = {}):
    """
    - Read/Get Records into MongoDB
    :param db_colln:
    :param qry_type: str: 'get_one'
    :param payload: dict: { 'first_name': 1, 'last_name': 1 }
    :param search_paramset: dict: { 'first_name': 'Ashish', 'last_name': 'Soni' }
    :return:
    """
    result = result_obj = None
    try:
        # logger.info(f'---read_records--START---')
        logger.info(f'---qry_type: {qry_type} ---search_paramset: {search_paramset} ---payload: {payload}')
        if qry_type == 'get_one':
            result_obj = db_colln.find_one()
        elif qry_type == 'get_all':
            result_obj = db_colln.find(search_paramset, payload)
        result = bdumps(list(result_obj))

    except Exception as ex:
        logger.critical('---create_records--funn---EXCEPTION---', exc_info=True)
        logger.critical(f'---create_records--funn---EXCEPTION--msg: {ex}')
    finally:
        logger.info(f'---result: {result}')
        return result


def update_records(db_colln, qry_type, where_dict: dict = {}, update_dict: dict = {}):
    """
    - Update Records into MongoDB
    :param db_colln:
    :param qry_type: str: 'update_one'
    :param where_dict: dict: {'last_name': 'Soni'}
    :param update_dict: dict: {'$set': {'is_active': False}}
    :return:
    """
    is_updated = False
    try:
        # logger.info(f'---update_records--START---')
        logger.info(f'---qry_type: {qry_type} ---where_dict: {where_dict} ---update_dict: {update_dict}')
        if qry_type == 'update_one':
            is_updated = True
            result = db_colln.update_one(where_dict, update_dict)
        elif qry_type == 'update_many':
            result = db_colln.update_many(where_dict, update_dict)
            is_updated = True

    except Exception as ex:
        logger.critical('---update_records--funn---EXCEPTION---', exc_info=True)
        logger.critical(f'---update_records--funn---EXCEPTION--msg: {ex}')
    finally:
        logger.info(f'---is_updated: {is_updated}')
        return is_updated


def delete_records(db_colln, qry_type, delete_dict: dict = {}):
    """
    - Delete Records into MongoDB
    :param db_colln:
    :param qry_type: str: 'update_one'
    :param delete_dict: dict: { 'first_name': 1, 'last_name': 1 }
    :return:
    """
    is_deleted = False
    try:
        # logger.info(f'---delete_records--START---')
        logger.info(f'---qry_type: {qry_type} ---delete_dict: {delete_dict}')
        if qry_type == 'delete_one':
            result = db_colln.delete_one(delete_dict)
            is_deleted = True
        elif qry_type == 'delete_many':
            result = db_colln.update_many(delete_dict)
            is_deleted = True

    except Exception as ex:
        logger.critical('---delete_records--funn---EXCEPTION---', exc_info=True)
        logger.critical(f'---delete_records--funn---EXCEPTION--msg: {ex}')
    finally:
        logger.info(f'---is_deleted: {is_deleted}')
        return is_deleted


def main():
    try:
        logger.info(f'---main--funn--START---')
        db_conn = create_db_conn('employees_db')
        db_collection = create_db_collection(db_conn, 'personal_details')

        # -> Create Records
        emp_details = {
            'first_name': 'Ashish',
            'last_name': 'Soni',
            'gender': 'male',
            'age': 23,
            'is_active': True
        }
        create_result = create_records(db_collection, emp_details)
        logger.debug(f'---create_result: {create_result}')

        # -> Read Records
        # get_records = read_records(db_collection, 'get_one')
        # get_records = read_records(db_collection, 'get_all')
        # get_records = read_records(db_collection, 'get_all', {}, {'last_name': 0})
        # get_records = read_records(db_collection, 'get_all', {}, {'first_name': 1})
        # get_records = read_records(db_collection, 'get_all', {}, {'_id' : 0, 'first_name': 1})
        # get_records = read_records(db_collection, 'get_all', {'first_name': 'Ashish', 'last_name': 'Soni'})
        # get_records = read_records(db_collection, 'get_all', {'first_name': 'Ashish', 'last_name': 'Soni'}, {'_id' : 0, 'first_name': 1})
        # logger.debug(f'---get_records: {get_records}')

        # -> Update Records
        # qry_type = 'update_one'
        # qry_type = 'update_many'
        # where_dict = {'last_name': 'Soni'}
        # update_dict = {'$set': {'is_active': False}}
        # update_results = update_records(db_collection, qry_type, where_dict, update_dict)
        # logger.debug(f'---update_results: {update_results}')

        # -> Deleted Records
        # qry_type = 'delete_one'
        # # qry_type = 'delete_many'
        # delete_dict = {'last_name': 'Soni'}
        # delete_results = delete_records(db_collection, qry_type, delete_dict)
        # logger.debug(f'---delete_results: {delete_results}')


    except Exception as ex:
        logger.critical('---main--funn---EXCEPTION---', exc_info=True)
        logger.critical(f'---main--funn---EXCEPTION--msg: {ex}')


if __name__ == '__main__':
    main()
