// Runs once per `docker compose up` (mongo-init service). Safe to re-run:
// it only initiates the replica set if needed, and only inserts institutions
// that don't exist yet (existing documents, including your edits, are left alone).

// 1. Replica set (change streams, and so Kafka Connect, require one)
try {
  rs.status();
  print("Replica set already initiated");
} catch (e) {
  // Member host must be the Compose service name so Kafka Connect can resolve it.
  rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "mongodb:27017" }] });
  print("Replica set initiated");
}

// 2. Wait until this node is PRIMARY (writes fail before then)
for (let i = 0; i < 60 && !db.hello().isWritablePrimary; i++) {
  sleep(1000);
}
if (!db.hello().isWritablePrimary) {
  throw new Error("MongoDB did not become PRIMARY within 60s");
}

// 3. Seed the institution registry
const registry = db.getSiblingDB("payment_metadata").institution_registry;

const institutions = [
  { institution_code: "BANK_UNIONBANK", institution_name: "UnionBank", rail_eligibility: ["INSTAPAY", "PESONET"], bank_code: "brankasunionbank", active: true },
  { institution_code: "BANK_BPI",       institution_name: "BPI",       rail_eligibility: ["INSTAPAY", "PESONET"], bank_code: "dobbpi",           active: true },
  { institution_code: "BANK_LANDBANK",  institution_name: "Landbank",  rail_eligibility: ["PESONET"],             bank_code: "brankaslandbank",  active: true },
  { institution_code: "EMI_GCASH",      institution_name: "GCash",     rail_eligibility: ["INSTAPAY"],            bank_code: null,               active: true },
  { institution_code: "EMI_MAYA",       institution_name: "Maya",      rail_eligibility: ["INSTAPAY"],            bank_code: null,               active: true },
];

registry.createIndex({ institution_code: 1 }, { unique: true });

const result = registry.bulkWrite(
  institutions.map((doc) => ({
    updateOne: {
      filter: { institution_code: doc.institution_code },
      update: { $setOnInsert: doc },
      upsert: true,
    },
  }))
);

print(`Institutions inserted: ${result.upsertedCount}, already present: ${institutions.length - result.upsertedCount}`);
print(`institution_registry now has ${registry.countDocuments()} documents`);
