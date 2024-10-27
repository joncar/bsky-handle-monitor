import * as WS from "ws";
import Database from "better-sqlite3";

/////////////////////////////////////////////////
enum CommitType {
    Create = "c",
    Delete = "d"
}
type CreateRecordCommit = {
    rev: string;
    type: CommitType.Create;
    collection: string;
    rkey: string;
    record: {
        '$type': string;
        createdAt: string;
        subject: {
            cid: string;
            uri: string;
        }
    }
}
type DeleteRecordCommit = {
    rev: string;
    type: CommitType.Delete;
    collection: string;
    rkey: string;
};
enum MessageType {
    Commit = "com",
    Identity = "id",
    Account = "acc"
}
type CommitMessage = {
    did: string;
    time_us: number;
    type: MessageType.Commit;
    commit: CreateRecordCommit | DeleteRecordCommit;
};
type IdentityMessage = {
    did: string;
    time_us: number;
    type: MessageType.Identity;
    identity: {
        did: string;
        handle: string;
        seq: number;
        time: string;
    }
};
type AccountMessage = {
    did: string;
    time_us: number;
    type: MessageType.Account;
    account: {
        active: boolean;
        did: string;
        seq: number;
        time: string;
    }
};
type Message = CommitMessage | IdentityMessage| AccountMessage;

/////////////////////////////////////////////////

class JetStreamClient {
    readonly url: string;
    #ws: WS.WebSocket | undefined;
    #reconnectMs: number;
    onmessage: (msg: Message) => void;
    cursor: number;

    constructor(url: string, onmessage: (msg: Message) => void) {
        this.url = url;
        this.onmessage = onmessage;
        this.cursor = 0;
        this.#reconnectMs = 1000;
        this.#connect();
    }

    #connect() {
        console.log('connecting...');
        this.#ws = new WS.WebSocket(
            this.url,
            {
                headers: {
                    'User-Agent': 'experimental client by @joncaruana.com'
                }
            }
        );

        this.#ws.on("open", () => {
            console.log(`open`);
        });

        this.#ws.on("message", async (data) => {
            try {
                let buffer: Buffer;
                if (data instanceof Buffer) {
                    buffer = data;
                } else if (data instanceof ArrayBuffer) {
                    buffer = Buffer.from(data);
                } else if (Array.isArray(data)) {
                    buffer = Buffer.concat(data);
                } else {
                    throw new Error(`Unknown message contents: ${data}`);
                }
                let msg = JSON.parse(buffer.toString()) as Message;
                if (msg === undefined) {
                    return;
                }
                this.cursor = msg.time_us;
                this.onmessage(msg);
            } catch (error) {
                console.log(`Message error: ${error}`);
            }
        });

        this.#ws.on("close", () => {
            console.log(`close`);
            this.#reconnectMs = Math.min(this.#reconnectMs * 2, 5 * 60000);
            setTimeout(this.#connect, this.#reconnectMs);
        });

        this.#ws.on("error", (error) => {
            console.log(`error: ${error}`);
        });
    }

    getExpectedCursor() {
        return Date.now() * 1000; // microseconds
    }

    reconnect() {
        this.#ws?.close();
    }
}

////////////////////////////

const db = new Database('handles.db');
db.pragma('journal_mode = WAL');

db.exec("CREATE TABLE IF NOT EXISTS handles (id INTEGER PRIMARY KEY, did TEXT UNIQUE, handle TEXT)");

const saveIdentityStmt = db.prepare("INSERT INTO handles (did, handle) VALUES (@did, @handle) ON CONFLICT (did) DO UPDATE SET handle = @handle");

let messageCount = 0;
let idCount = 0;
function processMsg(msg: Message) {
    messageCount++;
    switch (msg.type) {
        case MessageType.Identity:
            //console.log(`ID\t${msg.identity.handle}\t${msg.identity.did}`);
            idCount++;
            if (!msg.identity.handle.endsWith(".bsky.social") &&
                !msg.identity.handle.endsWith(".brid.gy")) {
                console.log(msg.identity.handle);
            }
            saveIdentityStmt.run({did: msg.did, handle: msg.identity.handle});
            break;
        case MessageType.Commit:
            //console.log(msg);
            switch (msg.commit?.type) {
                case CommitType.Create:
                    //console.log(`CREATE\t${msg.did}\t${msg.commit.collection}\t${msg.commit.rkey}`);
                    break;
                case CommitType.Delete:
                    //console.log(`DELETE\t${msg.did}\t${msg.commit.collection}\t${msg.commit.rkey}`);
                    break;
                case undefined:
                    //console.log(`EMPTY\t${msg.did}`);
                    break;
                default:
                    //console.log(`UNKNOWN\t${(msg.commit as any).type}`);
                    break;
            }
            break;
        case MessageType.Account:
            //console.log(`ACC\t${msg.account.did}\t${msg.account.active}`);
            break;
        default:
            console.log(`UNKNOWN\t${(msg as any).type}`);
            break;
    }
}

const client = new JetStreamClient(`wss://jetstream1.us-west.bsky.network/subscribe?wantedCollections=app.bsky.none`, processMsg);
const queryStatsStmt = db.prepare('SELECT COUNT(*) AS c FROM handles');

let lastMessageCount = 0;
const statsInterval = 10;
setInterval(() => {
    let stats = queryStatsStmt.get() as {c: number};
    let mps = ((messageCount - lastMessageCount) / statsInterval).toFixed(2);
    let expectedCursor = client.getExpectedCursor();
    let cursorSecondsBehind = ((expectedCursor - client.cursor) / 1000000);
    console.log(`Received ${messageCount} messages (${idCount} identity). ${mps} m/s. Saved ${stats.c} rows. Cursor is ${cursorSecondsBehind.toFixed(2)} seconds behind.`);
    if (lastMessageCount == messageCount && cursorSecondsBehind > 120) {
        console.log(`Connection appears stalled. Reconnecting...`);
        client.reconnect();
    }
    lastMessageCount = messageCount;
}, statsInterval * 1000);