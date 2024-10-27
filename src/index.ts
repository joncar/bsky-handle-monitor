import { Jetstream } from "@skyware/jetstream";
import Database from "better-sqlite3";

const db = new Database('handles.db');
db.pragma('journal_mode = WAL');

db.exec("CREATE TABLE IF NOT EXISTS handles (id INTEGER PRIMARY KEY, did TEXT UNIQUE, handle TEXT)");

const saveIdentityStmt = db.prepare("INSERT INTO handles (did, handle) VALUES (@did, @handle) ON CONFLICT (did) DO UPDATE SET handle = @handle");

const jetstream = new Jetstream({
    "wantedCollections": ["app.bsky.none"],
    "endpoint": "wss://jetstream1.us-west.bsky.network/subscribe"
});

function getExpectedCursor() {
    return Date.now() * 1000; // microseconds
}

jetstream.on("open", () => {
    console.log("Connected");
});
jetstream.on("close", () => {
    console.log("Disconnected");
});
jetstream.on("error", (error, cursor) => {
    console.log("Error: ", error);
    console.log("Last Cursor: ", cursor);
});

let lastCursor = 0;
let messageCount = 0;
let idCount = 0;
jetstream.on("identity", (event) => {
    lastCursor = event.time_us;
    messageCount += 1;

    const did = event.identity.did;
    const handle = event.identity.handle;

    if (handle === undefined) {
        return;
    }

    idCount++;
    if ((!handle.endsWith(".bsky.social") && !handle.endsWith(".brid.gy")) || did.startsWith("did:web:")) {
        console.log(`${handle} => ${did}`);
    }
    saveIdentityStmt.run({did: did, handle: handle});
});

const queryStatsStmt = db.prepare('SELECT COUNT(*) AS c FROM handles');

let lastMessageCount = 0;
const statsInterval = 10;
setInterval(() => {
    let stats = queryStatsStmt.get() as {c: number};
    let mps = ((messageCount - lastMessageCount) / statsInterval).toFixed(2);
    let expectedCursor = getExpectedCursor();
    let cursorSecondsBehind = ((expectedCursor - lastCursor) / 1000000);
    console.log(`Received ${messageCount} messages (${idCount} identity). ${mps} m/s. Saved ${stats.c} rows. Cursor is ${cursorSecondsBehind.toFixed(2)} seconds behind.`);
    /*if (lastMessageCount == messageCount && cursorSecondsBehind > 120) {
        console.log(`Connection appears stalled. Reconnecting...`);
        client.reconnect();
    }*/
    lastMessageCount = messageCount;
}, statsInterval * 1000);

jetstream.start();