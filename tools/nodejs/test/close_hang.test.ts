import 'chai-as-promised';
import {exec as _exec} from "child_process";
import fs from "fs-extra";
import path from "path";
import {Database, OPEN_READWRITE} from "..";
import {promisify} from "util";
import {expect} from "chai";

const exec = promisify(_exec);

it("close hang", async function main() {
  if (process.platform == 'win32') this.skip();

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
  try {
    await new Promise((resolve) =>
        db.exec("create table foo(bar int)", resolve),
    );
  } finally {
    await new Promise((resolve) => db.close(resolve));
  }

  // exit code 1 and stdout blank means no open handles
  await expect(exec(
      `lsof ${databasePath}`)).to.eventually.be.rejected.and.to.include({ 'code': 1 ,'stdout': ''});
});
