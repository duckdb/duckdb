import { exec, ExecException } from "child_process";
import fs from "fs-extra";
import path from "path";
import { Database, OPEN_READWRITE } from "..";

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

it("close hang", async function main() {
  const databasePath = path.join(__dirname, "tmp", "close_hang.db");
  const pathExists = await fs.pathExists(databasePath);
  if (pathExists) {
    await fs.remove(databasePath);
  }
  await fs.mkdirp(path.dirname(databasePath));

  const db = await new Promise<Database>((resolve, reject) => {
    let db: Database = new Database(
      databasePath,
      OPEN_READWRITE,
      (err: unknown) => (err ? reject(err) : resolve(db)),
    );
  });
  console.info("Database.create(%s)", databasePath);
  try {
    await new Promise((resolve) =>
      db.exec("create table foo(bar int)", resolve),
    );
  } finally {
    await new Promise((resolve) => db.close(resolve));
    console.info("Database.close(%s)", databasePath);
  }

  let fileOpen = true;
  do {
    exec(
      `lsof ${databasePath}`,
      (err: ExecException | null, stdout: string): void => {
        if (err?.code === 1) {
          fileOpen = false;
          console.info("Database file closed");
        } else {
          console.info("Database file still open. Details:\n%s", stdout);
        }
      },
    );
    await sleep(1_000);
  } while (fileOpen);
});
