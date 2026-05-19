import { MongoClient } from "mongodb";

const uri = process.env.MONGODB_URI;

export default async function handler(req, res) {

  const client = new MongoClient(uri);

  try {

    await client.connect();

    const db = client.db("kaltim_flood");

    const collection = db.collection("weather_data");

    const data = await collection.find({})
      .sort({ timestamp: -1 })
      .limit(50)
      .toArray();

    res.status(200).json(data);

  } catch (err) {

    res.status(500).json({
      error: err.message
    });

  } finally {

    await client.close();

  }
}
