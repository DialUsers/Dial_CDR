r"""
convert the SPI column, here it is the second column in row
####Author#### Dongdong Cheng, dongdong_cheng@infosys.com

####Change Log####
v1.0, 20180608, initialize the code
v1.1, 20180608, sequence execution no parallel
v1.2, 20180611, add global mapping
v1.3, 20180613, for POC, do not flush the global mapping into disk at each local mapping merging,
    only done once at the end of job controller
"""

####python standard library####
import os
import csv
from pathlib import Path
import hashlib
import shutil

####project module####
from shared_code import _print, source_file_folder, working_folder_data, working_folder_mapping, global_mapping_folder, \
    target_file_folder


####class####Exception####
class folder_exception(RuntimeError):
    """runtime error when access source folder"""
    pass


####class####Exception####begin####
class value_exception(RuntimeError):
    """hash value is abnormal."""
    pass


####class####
class source_folder():
    """
    it will scan the source folder and put all files under the folder to self._source_file_list[]

    :return
    self._source_file_list[]
    """

    def __init__(self, source_file_folder):
        self._source_file_folder = source_file_folder
        self._scan_source_files()

    def _scan_source_files(self):
        _print(4, 'INFO: scanning folder =' + self._source_file_folder)
        if not os.path.isdir(self._source_file_folder):
            raise folder_exception("source folder not exist. folder name=" + self._source_file_folder)
        self._source_file_list = []
        for source_file_path in Path(self._source_file_folder).iterdir():
            if source_file_path.is_file():  # only catch files
                self._source_file_list.append(source_file_path)
            else:
                _print(9, 'DEBUG: ignore non-file object when scan source folder.')
        _print(4, 'INFO: total file found =' + str(len(self._source_file_list)))


class global_mapping():
    r"""
    single object contains global mapping
    currently we hold global mapping in memory, and global mapping is single file
    """

    def __init__(self, job_controller):
        self._global_mapping_folder = job_controller._global_mapping_folder
        self._global_mapping_file = 'global_mapping.csv'
        self._global_mapping_file_fullname = self._global_mapping_folder + '\\' + self._global_mapping_file

        self._hash = hashlib.sha256()

        self._mapping = {}
        self._revert_mapping = {}
        self._duplicated_hash_value = {}

        self._read_from_disk()

    def _add_hash_entry(self, entry_key, entry_value):
        """
        check the input hash (key) is already in the _mapping data or not
        return existing or new generate hash_value for input key

        :return
        (0, ), key already exist, nothing changed in _mapping and other
        (1, ), new key, hash value no conflict
        (2, new hash_value), new key, hash value conflict
        """

        if entry_key in self._mapping:
            if entry_value == self._mapping[entry_key]:
                return (0,)
            else:
                if entry_value in self._duplicated_hash_value:
                    # already in duplicated list
                    self._duplicated_hash_value[entry_value]['conflict_count'] += 1
                    new_conflict_count = self._duplicated_hash_value[entry_value]['conflict_count']
                    new_hash_value = entry_value + '_' + str(new_conflict_count)
                    self._duplicated_hash_value[entry_value][entry_key] = new_hash_value
                else:
                    # build new duplicated entry
                    self._duplicated_hash_value[entry_value] = {}
                    self._duplicated_hash_value[entry_value]['conflict_count'] = 2
                    new_hash_value = entry_value + '_1'
                    self._duplicated_hash_value[entry_value][self._revert_mapping[entry_value]] = entry_value
                    self._duplicated_hash_value[entry_value][entry_key] = new_hash_value
                # always have new_hash_value
                self._revert_mapping[new_hash_value] = entry_key
                self._mapping[entry_key] = new_entry_value
                return (2, new_hash_value)
        else:
            self._mapping[entry_key] = entry_value
            self._revert_mapping[entry_value] = entry_key
            return (1,)

    def _read_from_disk(self):
        """
        file has 3 fields:
        key, original_hash_value, extend_hash_value(optional)
        only when value met conflict, we use extend_hash_value. in POC we not detect any conflict
        """
        _print(4, 'INFO: start loading global mapping file =' + self._global_mapping_file_fullname)
        if os.path.isfile(self._global_mapping_file_fullname):
            _global_mapping_file_handler = open(self._global_mapping_file_fullname, 'r', encoding='utf-8', newline='')
            _CSVReader = csv.reader(_global_mapping_file_handler, delimiter=',', quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)

            total_row_count = 0
            extend_row_count = 0
            for data_row in _CSVReader:
                total_row_count += 1
                entry_key = data_row[0]
                entry_value = data_row[1]
                extend_value = data_row[2]
                hash_object = hashlib.sha256(entry_key.encode('utf-8', 'ignore'))
                hash_value = hash_object.hexdigest()
                if entry_value != hash_value:
                    raise value_exception("hash value abnormal, line=" + str(total_row_count) + ',\t SPI=' + entry_key)
                if extend_value is not None and extend_value != '':
                    extend_row_count += 1
                    self._mapping[entry_key] = extend_value
                    self._revert_mapping[extend_value] = entry_key
                    if entry_value in self._duplicated_hash_value:
                        self._duplicated_hash_value[entry_value]['conflict_count'] = 1
                    else:
                        self._duplicated_hash_value[entry_value]['conflict_count'] += 1
                    self._duplicated_hash_value[entry_value][entry_key] = extend_value
                else:
                    self._mapping[entry_key] = entry_value
                    self._revert_mapping[entry_value] = entry_key

            _global_mapping_file_handler.close()
            _print(4, 'INFO: global mapping file loaded, total row count =' + str(
                total_row_count) + ',\t duplicated value found =' + str(extend_row_count))
        else:
            _print(4, 'WARNING: global mapping file not exist. leave the mapping dict as empty.')

    def _write_to_disk(self):

        _print(4, 'INFO: start writing global mapping file =' + self._global_mapping_file_fullname)
        _global_mapping_file_handler = open(self._global_mapping_file_fullname, 'w', encoding='utf-8', newline='')
        _CSVWriter = csv.writer(_global_mapping_file_handler, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for SPI_original in self._mapping:
            _CSVWriter.writerow((SPI_original, self._mapping[SPI_original], ''))
        _global_mapping_file_handler.close()
        _print(4, 'INFO: global mapping file written, row count =' + str(len(self._mapping)))

    def merge_dict(self, local_mapping_dict):
        _print(4, 'INFO: before merge, global mapping row count =' + str(
            len(self._mapping)) + ',\t local mapping row count =' + str(len(local_mapping_dict)))
        _local_duplicated_mapping = {}
        added_count = 0
        for SPI_original in local_mapping_dict:
            result = self._add_hash_entry(SPI_original, local_mapping_dict[SPI_original])
            if result[0] > 0:
                added_count += 1
            if result[0] == 2:
                _local_duplicated_mapping[SPI_original] = result[1]

        _print(4, 'INFO: after merge, global mapping row count =' + str(len(self._mapping))
               + ',\t new key added to global =' + str(added_count)
               + ',\t duplicated row count in local =' + str(len(_local_duplicated_mapping))
               )
        return _local_duplicated_mapping


class job():
    r"""
    each object correspond to one source file's processing process
    """

    def __init__(self, source_file_path, job_controller):
        self._source_file_path = source_file_path
        self._job_controller = job_controller
        self._source_file_name = self._source_file_path.name
        self._working_data_file_fullname = self._job_controller._working_folder_data + '\\' + self._source_file_name
        self._target_data_file_fullname = self._job_controller._target_file_folder + '\\' + self._source_file_name
        self._working_mapping_file_fullname = self._job_controller._working_folder_mapping + '\\' + self._source_file_name

        self._local_mapping = {}  # hold this in memory under sequence mode

    def _local_hash(self):
        _source_file_handler = open(self._source_file_path, 'r', encoding='utf-8', newline='')
        _print(4, 'INFO: start local hash for source file =' + self._source_file_name)
        _working_data_file_handler = open(self._working_data_file_fullname, 'w', encoding='utf-8', newline='')
        _CSVReader = csv.reader(_source_file_handler, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        _CSVWriter = csv.writer(_working_data_file_handler, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        total_row_count = 0
        for data_row in _CSVReader:
            total_row_count += 1
            SPI_original = data_row[1] # This will pick Field 1 for Hashing
            SPI_original_1 = data_row[2] # This will pick Field 2 for Hashing
            hash_object = hashlib.sha256(SPI_original.encode('utf-8', 'ignore'))
            hash_object_1 = hashlib.sha256(SPI_original_1.encode('utf-8', 'ignore'))
            SPI_hashed = hash_object.hexdigest()
            SPI_hashed_1 = hash_object_1.hexdigest()
            self._local_mapping[SPI_original] = SPI_hashed
            self._local_mapping[SPI_original_1] = SPI_hashed_1
            _CSVWriter.writerow((data_row[0],) + (SPI_hashed,) + (SPI_hashed_1,) + tuple(data_row[3:]))  # use tuple add here-- Gokul - This will populate only the hashed fields
        _source_file_handler.close()
        _working_data_file_handler.close()
        _print(4, 'INFO: local hash data file write to =' + self._working_data_file_fullname + ',\t total rows =' + str(
            total_row_count))

        _working_mapping_file_handler = open(self._working_mapping_file_fullname, 'w', encoding='utf-8', newline='')
        _CSVWriter = csv.writer(_working_mapping_file_handler, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for SPI_original in self._local_mapping:
            _CSVWriter.writerow((SPI_original, self._local_mapping[SPI_original]))
        _working_mapping_file_handler.close()
        _print(4,
               'INFO: local hash mapping file write to =' + self._working_mapping_file_fullname + ',\t total rows =' + str(
                   len(self._local_mapping)))

    def _global_unique(self):
        _local_duplicated_mapping = self._job_controller._global_mapping.merge_dict(self._local_mapping)
        #v1.3, for POC purpose, we not need flush the global mapping object into disk every time
        #self._job_controller._global_mapping._write_to_disk()

        if len(_local_duplicated_mapping) > 0:
            _print(4, 'WARNING: duplication found in global, number=' + len(_local_duplicated_mapping))
        else:
            shutil.move(self._working_data_file_fullname, self._target_data_file_fullname)
            _print(4,
                   'INFO: no hash conflict, simply move the hashed data file, from=' + self._working_data_file_fullname
                   + ',\t to=' + self._target_data_file_fullname)


class controller():
    r"""
    controller use to control all the work load and job assignment,
    but current we not implement it for parallel programming
    """

    def __init__(self, source_file_folder, working_folder_data, working_folder_mapping, global_mapping_folder,
                 target_file_folder):
        self._source_file_folder = source_file_folder
        self._working_folder_data = working_folder_data
        self._working_folder_mapping = working_folder_mapping
        self._global_mapping_folder = global_mapping_folder
        self._target_file_folder = target_file_folder

        self._check_target_folder()
        self._check_global_mapping_folder()
        self._check_working_folder()

        self._global_mapping = global_mapping(self)

        self._source_folder = source_folder(self._source_file_folder)
        for source_file_path in self._source_folder._source_file_list:
            self._assign_worker(job(source_file_path, self))

        #v1.3, for POC purpose, only flush it at the end
        self._global_mapping._write_to_disk()

    def _assign_worker(self, this_job):
        """
        currently we use sequential executing mode, so there only one worker
        """
        this_job._local_hash()
        this_job._global_unique()

    def _check_target_folder(self):
        if not os.path.isdir(self._target_file_folder):
            os.makedirs(self._target_file_folder)
            _print(4, 'INFO: target folder created =' + self._target_file_folder)
        elif len(os.listdir(self._target_file_folder)) > 0:
            raise folder_exception("target file folder is not empty. folder name=" + self._target_file_folder)

    def _check_global_mapping_folder(self):
        if not os.path.isdir(self._global_mapping_folder):
            os.makedirs(self._global_mapping_folder)
            _print(4,
                   'INFO: global mapping folder created only for first time execution =' + self._global_mapping_folder)
        else:
            _print(4, 'INFO: global mapping folder exist, folder name =' + self._global_mapping_folder)

    def _check_working_folder(self):
        if not os.path.isdir(self._working_folder_data):
            os.makedirs(self._working_folder_data)
            _print(4, 'INFO: working folder for data created =' + self._working_folder_data)
        else:
            _print(4,
                   'WARNING: working folder for data already existed, file will be overwritten. folder name =' + self._working_folder_data)

        if not os.path.isdir(self._working_folder_mapping):
            os.makedirs(self._working_folder_mapping)
            _print(4, 'INFO: working folder for mapping created =' + self._working_folder_mapping)
        else:
            _print(4,
                   'WARNING: working folder for mapping already existed, file will be overwritten. folder name  =' + self._working_folder_mapping)


####unit test####
if __name__ == '__main__':  # unit testing case

    # initial program
    _print(2, 'INFO: data generator unit testing starting...')
    my_control = controller(source_file_folder, working_folder_data, working_folder_mapping, global_mapping_folder,
                            target_file_folder)

    _print(2, 'INFO: data generator unit testing finished')
