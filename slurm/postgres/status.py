 '''
Postgres tables for the PDC CWL Workflow
'''
import postgres.utils
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy import MetaData, Table
from sqlalchemy import Column, String


def get_status(upload_exit, cwl_exit, upload_file_location, upload_dir_location, logger):
    """ update the status of job on postgres """
    loc = 'UNKNOWN'
    status = 'UNKNOWN'
    if upload_exit == 0:
        loc = upload_file_location
        if cwl_exit == 0:
            status = 'COMPLETED'
            logger.info("uploaded all files to object store. The path is: %s" % upload_dir_location)
        else:
            status = 'CWL_FAILED'
            logger.info("CWL failed. The log path is: %s" % upload_dir_location)
    else:
        loc = 'Not Applicable'
        if cwl_exit == 0:
            status = 'UPLOAD_FAILURE'
            logger.info("Upload of files failed")
        else:
            status = 'FAILED'
            logger.info("CWL and upload both failed")
    return(status, loc)


# class State(object):
#     pass


class Files(object):
    pass


def get_bams(engine, input_table):
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()
    meta = MetaData(engine)
    # read the input table
    data = Table(input_table, meta, autoload=True, autoload_with=engine)
    mapper(Files, data)
    # count = 0
    # s = dict()
    bams = session.query(Files).all()
    # for row in cases:
    #     s[count] = [row.input_id,
    #                 row.project,
    #                 row.md5sum,
    #                 row.s3_url,
    #                 row.s3_profile,
    #                 row.s3_endpoint]
    #     count += 1

    return bams


def get_case(engine, input_table, status_table, input_primary_column="id"):
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()
    meta = MetaData(engine)
    # read the input table
    data = Table(input_table, meta, Column(input_primary_column, String, primary_key=True), autoload=True)
    mapper(Files, data)
    count = 0
    s = dict()
    cases = session.query(Files).all()
    if status_table == "None":
        for row in cases:
            s[count] = [row.input_id,
                        row.project,
                        row.md5,
                        row.s3_url,
                        row.s3_profile,
                        row.s3_endpoint]
            count += 1
    else:
        # read the status table
        state = Table(status_table, meta, autoload=True)
        mapper(State, state)
        for row in cases:
            completion = session.query(State).filter(State.input_id[0] == row.input_id).first()
            if completion == None or not(completion.status == 'COMPLETED'):
                s[count] = [row.input_id,
                            row.project,
                            row.md5,
                            row.s3_url,
                            row.s3_profile,
                            row.s3_endpoint]
                count += 1
    return s


def get_case_from_status(engine, status_table, input_primary_column, profile, endpoint, input_table=None):
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()
    meta = MetaData(engine)

    # read the status table
    state = Table(status_table, meta, autoload=True)
    mapper(State, state)

    # collect input information from status and/or input tables
    count = 0
    s = dict()
    cases = session.query(State).all()
    for row in cases:
        if row.status == 'COMPLETED':
            if not input_table or (input_table and row.input_table == input_table):
                s[count] = [row.output_id,
                            row.project,
                            row.md5,
                            row.s3_url,
                            profile,
                            endpoint]
                count += 1

    return s

