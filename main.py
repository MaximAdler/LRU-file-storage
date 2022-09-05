from sys import getsizeof
from typing import Dict, Optional, Any


class MongoDB:
    def __init__(self):
        pass

    def get_one(self, collection: str, **rules) -> Optional[Dict[str, str]]:
        """Gets data from the collection"""
        pass

    def create(self, collection: str, data: Dict[str, Any]) -> str:
        """Creates record to the collection. Returns id"""
        pass

    def modify(self, collection: str, data: Dict[str, Any], **rules) -> None:
        """Modifies record to the collection"""
        pass

    def delete(self):
        """Deletes record"""
        pass


class Bucket:
    def __init__(self):
        """Metadata for S3 interaction"""
        self.connection = None

    def __enter__(self) -> 'Bucket':
        self.connection = self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.connection = None

    def connect(self) -> object:
        """Connect to s3"""
        pass

    def get_used_space(self, user_id: int) -> float:
        """Gets amount of space that used on S3 bucket for user"""
        pass

    def get_file_size(self, path: str) -> float:
        """Gets file size"""
        pass

    def get_file(self, path: str) -> str:
        """Gets file from s3 bucket"""
        pass

    def add_file(self, path: str, file: str) -> None:
        """Adds file to s3 bucket"""
        pass

    def rewrite_file(self, path: str, value: str) -> str:
        """Rewrites file on s3 bucket"""
        pass

    def delete_file(self, path: str) -> None:
        """Removes file from s3 bucket"""
        pass


class LRUStorage:
    def __init__(
        self, user_id: int, chat_id: str, mongo: 'MongoDB' = MongoDB, s3: 'Bucket' = Bucket, capacity: int = 5242880
    ):
        """
        :capacity - maximum amount of MB for user_id (5TB)
        :user_id - ID of user who sends the file
        :chat_id - hash of the chat
        :mongo - class of MongoDB connector
        :s3 - class of S3 Bucket storage connector
        """
        self.capacity = capacity
        self.user_id = user_id
        self.chat_id = chat_id
        self.__mongo = mongo()
        self.__s3 = s3()
        self.router = self._get_router()

    def _get_head(self) -> Dict[str, Any]:
        """Gets head from MongoDB. If no head, then it creates a new one"""

        # record: id (str), user_id (int), filename (str|None), next_id (str|None), prev_id (str|None), head (bool)
        head_record = self.__mongo.get_one(collection='lru_node', user_id=self.user_id, head=True)

        if head_record is None:
            head_id = self.__mongo.create(
                collection='lru_node',
                data=dict(user_id=self.user_id, head=True)
            )
            self.__mongo.modify(
                collection='lru_node',
                data=dict(next_id=head_id, prev_id=head_id),
                id=head_id
            )
            return {
                'id': head_id,
                'user_id': self.user_id,
                'filename': None,
                'next_id': head_id,
                'prev_id': head_id,
                'head': True
            }

        return head_record

    def _get_node(self, _id: str) -> Optional['LRUNode']:
        """Gets node from MongoDB"""

        # record: id (str), user_id (int), filename (str), next_id (str), prev_id (str)
        record = self.__mongo(collection='lru_node', id=_id)
        return record

    def _get_router(self) -> Dict[str, str]:
        """Gets router (hashmap) from MongoDB"""

        # record: id (str), user_id (int), router ({filename: lru_head.id, ...})
        record = self.__mongo.get_one(collection='lru_router', user_id=self.user_id)
        return record['router']

    def get(self, filename: str) -> Optional[str]:
        """Gets file from s3 bucket and moves file to the most frequent used position in MongoDB"""
        lru_node_id = self.router.get(filename, None)

        if not lru_node_id:
            return None

        # record: id (str), user_id (int), filename (str), next_id (str), prev_id (str), head (str)
        node = self.__mongo.get_one(collection='lru_node', id=lru_node_id)
        self.__remove(node)
        self.__add(node)

        return self.__s3.get_file(path=filename)

    def __get_file_size(self, filename: str = None, file: str = None) -> float:
        if not filename and file:
            raise Exception('filename or file should be provided')

        if filename:
            return self.__s3.get_file_size(path=filename)

        return getsizeof(file) / (2**20)  # megabytes

    def __allocate_space(self, file_size: float, reallocated_space: float = 0.0) -> None:
        """Allocates space for a new file. If it is not enough space - it removes least used"""
        if file_size > self.capacity:
            raise Exception('File size is too large.')

        while self.capacity - self.__s3.get_used_space() - reallocated_space < file_size:
            tail = self._get_head()
            last_node = self.__mongo.get_one(
                collection='lru_node',
                id=tail['prev_id']
            )
            self.__remove(last_node['id'])
            self.router.pop(last_node['filename'])
            self.router = self.__mongo.modify(
                collection='lru_router',
                data=dict(router=self.router),
                user_id=self.user_id
            )
            self.__s3.delete_file(path=last_node['filename'])

    def put(self, filename: str, file: str) -> None:
        """Puts file to s3 bucket as the most frequent used file"""
        lru_node_id = self.router.get(filename, None)
        new_file_size = self.__get_file_size(file=file)

        if lru_node_id:
            self.__allocate_space(file_size=new_file_size, reallocated_space=self.__get_file_size(filename=filename))
            node = self.__mongo.get_one(collection='lru_node', id=lru_node_id)
            self.__remove(node)
            self.__s3.rewrite_file(path=filename, value=file)
            self.__add(node)
            return

        self.__allocate_space(file_size=new_file_size)
        # record: id (str), user_id (int), filename (str|None), next_id (str|None), prev_id (str|None), head (bool)
        new_node_id = self.__mongo.create(
            collection='lru_node',
            data=dict(user_id=self.user_id, filename=filename, next_id=None, prev_id=None, head=False)
        )
        self.__s3.add_file(path=filename, file=file)
        self.__add(
            {
                'id': new_node_id,
                'user_id': self.user_id,
                'filename': filename,
                'next_id': None,
                'prev_id': None,
                'head': True
            }
        )
        self.router[filename] = new_node_id
        self.__mongo.modify(
            collection='lru_router',
            data=dict(router=self.router),
            user_id=self.user_id
        )

    def __remove(self, node: Dict[str, Any]) -> None:
        """Removes node from Doubly Linked List"""
        prev = node['prev_id']
        next_ = node['next_id']
        self.__mongo.modify(
            collection='lru_node',
            data=dict(next_id=next_),
            id=prev
        )
        self.__mongo.modify(
            collection='lru_node',
            data=dict(prev_id=prev),
            id=next_
        )

    def __add(self, node: Dict[str, Any]) -> None:
        """Add node to DDL"""
        head = self._get_head()
        current = head['next_id']
        self.__mongo.modify(
            collection='lru_node',
            data=dict(next_id=node['id']),
            id=head['id']
        )
        self.__mongo.modify(
            collection='lru_node',
            data=dict(prev_id=head['id'], next_id=current['id']),
            id=node['id']
        )
        self.__mongo.modify(
            collection='lru_node',
            data=dict(prev_id=node['id']),
            id=current['id']
        )
