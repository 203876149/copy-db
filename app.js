const { DataSource } = require("typeorm");
const dotenv = require("dotenv");

dotenv.config();

const dbA = new DataSource({
    type: "mysql",
    connectorPackage: "mysql2",
    host: process.env.HOST,
    port: process.env.PORT,
    username: 'root',
    password: process.env.PASSWORD,
    database: process.env.DBA,
    entities: ["./entities/*.js"],
    synchronize: false,
});

const dbB = new DataSource({
    type: "mysql",
    connectorPackage: "mysql2",
    host: process.env.HOST,
    port: process.env.PORT,
    username: 'root',
    password: process.env.PASSWORD,
    database: process.env.DBB,
    entities: ["./entities/*.js"],
    synchronize: false,
});

const skipTables = ["module"];



async function initializeDatabase(db, name) {
    try {
        console.log(`Connecting to ${name} at ${db.options.host}:${db.options.port}...`);
        await db.initialize();
        console.log(`${name} connected successfully.`);
    } catch (error) {
        console.error(`Failed to connect to ${name}:`, error);
        process.exit(1); // Exit script if connection fails
    }
}

async function copyData() {
    await initializeDatabase(dbA, "DB A");
    await initializeDatabase(dbB, "DB B");

    try {
        // Disable foreign key checks before inserting data
        await dbA.query("SET FOREIGN_KEY_CHECKS = 0");
        await dbB.query("SET FOREIGN_KEY_CHECKS = 0");

        // Get all mainTables ordered by parent-first structure
        const mainTables = await dbB.query(
            `SELECT DISTINCT TABLE_NAME 
                     FROM information_schema.KEY_COLUMN_USAGE 
                     WHERE TABLE_SCHEMA = ? 
                     AND COLUMN_NAME = 'id' 
                     AND CONSTRAINT_NAME = 'PRIMARY' 
                     ORDER BY TABLE_NAME`,
            [process.env.DBB]
        );

        for (const tableObj of mainTables) {
            if (skipTables.includes(Object.values(tableObj)[0])) {
                console.log(`Skipping table ${Object.values(tableObj)[0]}`);
                continue;
            }
            const tableName = '`' + Object.values(tableObj)[0] + '`';
            console.log(`Processing table: ${tableName}`);

            // Get max ID from DB B
            let maxId = 0;
            try {
                const [{ maxIdA }] = await dbA.query(`SELECT COALESCE(MAX(id), 0) AS maxIdA FROM ${tableName}`);
                const [{ maxIdB }] = await dbB.query(`SELECT COALESCE(MAX(id), 0) AS maxIdB FROM ${tableName}`);
                maxId = Math.max(maxIdA, maxIdB);
            } catch (error) {
                console.error(`Error getting max ID for table ${tableName}:`, error);
                continue;
            }
            if (typeof maxId !== 'number' || !maxId) {
                console.log(`No data found in table ${tableName}`);
            }

            // Update IDs in DB A
            console.log(`Updating IDs in ${tableName} by ${maxId}`);
            await dbB.query(`UPDATE ${tableName} SET id = id + ?`, [maxId]);

            // Get foreign keys referencing this table
            const foreignKeys = await dbB.query(
     `SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_NAME = ${'\'' + Object.values(tableObj)[0] + '\''} AND TABLE_SCHEMA = ${'\'' + process.env.DBB + '\''}`);

            for (const fk of foreignKeys) {
                const fKTableName = '`' + fk.TABLE_NAME + '`';
                const fKColeName = '`' + fk.COLUMN_NAME + '`';
                console.log(`Updating foreign key ${fk.COLUMN_NAME} in ${fk.TABLE_NAME}`);
                await dbB.query(`UPDATE ${fKTableName} SET ${fKColeName} = ${fKColeName} + ?`, [maxId]);
            }
        }


        // Get all tables ordered by parent-first structure
        const tables = await dbB.query(
            `SELECT DISTINCT TABLE_NAME 
                     FROM information_schema.KEY_COLUMN_USAGE 
                     WHERE TABLE_SCHEMA = ? 
                     ORDER BY TABLE_NAME`,
            [process.env.DBB]
        );
        for (const tableObj of tables) {
            const tableName = Object.values(tableObj)[0];
            if (skipTables.includes(tableName)) {
                console.log(`Skipping table ${Object.values(tableObj)[0]}`);
                continue;
            }
            console.log(`Processing table: ${tableName}`);

            // Step 1: Get column names for DB A and DB B
            const DBAName = '\'' + process.env.DBA + '\'', DBBName = '\'' + process.env.DBB + '\'';

            const columnsA = await dbA.query(`SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ${DBAName} AND TABLE_NAME = ${ '\'' + tableName + '\''}`);
            const columnsB = await dbB.query(`SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ${DBBName} AND TABLE_NAME = ${ '\'' + tableName + '\''}`);

            // Convert to arrays of column names
            const columnsAList = columnsA.map(col => '`' + col.COLUMN_NAME + '`');
            const columnsBList = columnsB.map(col => '`' + col.COLUMN_NAME + '`');

            // Find matching columns (ignoring order)
            const matchedColumns = columnsAList.filter(col => columnsBList.includes(col));

            if (matchedColumns.length === 0) {
                console.log(`Skipping table ${tableName} (no matching columns)`);
                continue;
            }

            // Convert column list to SQL format: `col1, col2, col3`
            const columnsSQL = matchedColumns.join(", ");
            await dbA.query("SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION'");
            // Step 2: Insert data with matched column names
            const insertQuery = `
                INSERT INTO ${process.env.DBA}.${tableName} (${columnsSQL})
                SELECT ${columnsSQL} FROM ${process.env.DBB}.${tableName};
            `;

            console.log(`Executing: ${insertQuery}`);
            try {
                await dbA.query(insertQuery);
            } catch (error) {
                console.error(`Error copying data for table ${tableName}:`, error);
            }
            console.log(`Data copied for table: ${tableName}`);
        }

        // Re-enable foreign key checks after inserting data
        await dbA.query("SET FOREIGN_KEY_CHECKS = 1");
        await dbB.query("SET FOREIGN_KEY_CHECKS = 1");

        console.log("Data migration completed!");
    } catch (error) {
        console.error("Error during migration:", error);
    } finally {
        await dbA.destroy();
        await dbB.destroy();
    }
}

copyData();

//mysql -h localhost -u root --password="aa123123" db2 < remote_dump_app.sql
//mysql -h localhost -u root --password="aa123123" syncurio < remote_dump_beta.sql
/*
UPDATE db2.user u
JOIN syncurio.user s ON u.email = s.email
SET u.email = CONCAT('1', u.email);
 */
