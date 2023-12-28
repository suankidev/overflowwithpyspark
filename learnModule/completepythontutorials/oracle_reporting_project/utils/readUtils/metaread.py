from configparser import ConfigParser
import os


def set_config_path():
    config = ConfigParser()
    config_file_path = os.getenv('config_file') + "\\meta_config.conf"
    config.read(config_file_path)
    return config


def get_db_meta_data(source_id, logger):
    logger.info("fetching the detail from oracle for the feed id {}.".format(source_id))
    config = set_config_path()
    config_section = config['ORACLE_' + source_id]
    feed_id = config_section.get('ID', 'default_id')
    table_name = config_section.get('TABLE', 'default_table')
    return feed_id, table_name


def get_db_meta_data_dictionary(source_id, logger):
    logger.info("fetching the detail from oracle for the feed id {}.".format(source_id))
    config = set_config_path()
    config_section = config['ORACLE_' + source_id]
    return dict(config_section)


def get_file_meta_data(source_id, logger):
    logger.info("fetching the detail from oracle for the feed id {}.".format(source_id))
    config = set_config_path()
    config_section = config['FILE_' + source_id]
    feed_id = config_section.get('ID', 'default_id')
    table_name = config_section.get('TABLE', 'default_table')
    return feed_id, table_name


def get_file_meta_data_dictionary(source_id, logger):
    logger.info("fetching the detail from oracle for the feed id {}.".format(source_id))
    config = set_config_path()
    config_section = config['FILE_' + source_id]
    return dict(config_section)
