


# from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
# from sqlalchemy.orm import declarative_base, sessionmaker
# from datetime import datetime

# Base = declarative_base()
# class Response(Base):
#     __tablename__ = 'tasks'

#     id = Column(Integer, primary_key=True)
#     task_id = Column(String)
#     status = Column(String)
#     time = Column(DateTime)
#     collection_name = Column(String)
#     uploads_dir = Column(String)
#     file_name = Column(String)
#     # result = Column(Text)
#     # successful = Column(Integer)

# engine = create_engine('sqlite:///task.db', echo=True)
# Base.metadata.create_all(bind=engine)
# Session = sessionmaker(bind=engine)

# def store_responses(responses):
#     session = Session()
#     for response in responses:
#         db_response = Response(
#             task_id=response['task_id'],
#             status=response['status'],
#             time=response['time'],
#             collection_name=response['collection_name'],
#             uploads_dir=response['uploads_dir'],
#             file_name=response['file_name'],
#             # result=str(response['result']),
#             # successful=int(response['successful'])
#         )
#         session.add(db_response)
#     session.commit()
#     session.close()

# def get_responses_from_db():
#     session = Session()
#     responses = session.query(Response).all()
#     session.close()
#     return [
#         {
#             'id': response.id,
#             'task_id': response.task_id,
#             'status': response.status,
#             'time': response.time.isoformat() if response.time else None,
#             'collection_name': response.collection_name,
#             'uploads_dir': response.uploads_dir,
#             'file_name': response.file_name,
#             # 'result': response.result,
#             # 'successful': bool(response.successful)
#         }
#         for response in responses
#     ]


from datetime import datetime
from pymongo import MongoClient

# MongoDB connection string
MONGO_URI = "mongodb+srv://ikegai:ikegai%40123456@cluster0.l2apier.mongodb.net"

client = MongoClient(MONGO_URI)
db = client.celery_tasks
collection = db.tasks

def store_responses(responses):
    for response in responses:
        document = {
            'task_id': response['task_id'],
            'status': response['status'],
            'time': response['time'],
            'collection_name': response['collection_name'],
            'uploads_dir': response['uploads_dir'],
            'file_name': response['file_name'],
            # 'result': str(response['result']),
            # 'successful': int(response['successful'])
        }
        collection.insert_one(document)

def get_responses_from_db():
    responses = []
    for document in collection.find():
        response = {
            'id': str(document['_id']),
            'task_id': document['task_id'],
            'status': document['status'],
            'time': document['time'].isoformat() if document['time'] else None,
            'collection_name': document['collection_name'],
            'uploads_dir': document['uploads_dir'],
            'file_name': document['file_name'],
            # 'result': document['result'],
            # 'successful': bool(document['successful'])
        }
        responses.append(response)
    return responses
