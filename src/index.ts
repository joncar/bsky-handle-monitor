import * as WS from "ws";

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

    constructor(url: string, onmessage: (msg: Message) => void) {
        this.url = url;
        this.onmessage = onmessage;
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
}

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

const client = new JetStreamClient(`wss://jetstream.atproto.tools/subscribe?wantedCollections=app.bsky.none`, processMsg);

let lastMessageCount = 0;
setInterval(() => {
    let mps = ((messageCount - lastMessageCount) / 60).toFixed(2);
    console.log(`Received ${messageCount} messages (${idCount} identity). ${mps} messages per second`);
    lastMessageCount = messageCount;
}, 60000);