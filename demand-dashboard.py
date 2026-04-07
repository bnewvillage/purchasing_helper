import firebase_admin
from firebase_admin import credentials, firestore

cred = credentials.Certificate("demand-dashboard-7f817-firebase-adminsdk-fbsvc-4292a722de.json")
firebase_admin.initialize_app(cred)

db = firestore.client()
print("Firestore Connected Successfully")

db.collection("test").document("hello").set({
    "message":"it works"
})
print("Write successful")

doc = db.collection("test").document("hello").get()
print(doc.to_dict())