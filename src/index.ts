import { McpServer, ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import mysql, { ResultSetHeader, RowDataPacket } from 'mysql2/promise';
import { ReadResourceResult } from "@modelcontextprotocol/sdk/types.js";
import { editorConnectionDetails, viewerConnectionDetails } from "./secrets/connectionDetails.js";
import { promises as fs } from "fs";

const server = new McpServer({
        name: 'dbman',
        version: '1.0.0'
    }, {
        capabilities: {
            resources: {},
            tools: {}
        }
    }
);

// Retrieve all databases in computer
server.registerResource(
    "all-databases",
    "internal://schemas/all-databases-schema",
    {
        title: 'Retrieve all databases',
        description: `
            Retrieve all databases in the system. Use this resource to list all databases 
            and instantiate resources of each database.
            Before creating a query, you MUST call this resource and db-schema in order to get complete overview of 
            databases, tables and their fields
        `,
        mimeType: 'text/plain'
    },
    async (uri): Promise<ReadResourceResult> => {
        let conn: undefined | mysql.Connection;
        
        let listOfDBs: Array<string> = [];
        const systemDBs: Set<string> = new Set(['information_schema', 'mysql', 'performance_schema', 'sys']);

        try {
            conn = await mysql.createConnection(viewerConnectionDetails);
            let [rows] = await conn.query<RowDataPacket[]>("SHOW DATABASES");
            await conn.end();
            if (rows.length > 0) {
                listOfDBs = rows.filter((db) => !systemDBs.has(db['Database']))
                            .map((db) => db['Database']);

                if (listOfDBs.length > 0) {
                    return {
                        contents: [
                            {
                                uri: uri.href,
                                text: listOfDBs.toString(),
                                mimeType: 'text/plain'
                            }
                        ]
                    };
                }

                return {
                    contents: [
                        {
                            uri: uri.href,
                            text: "No databases in your system!",
                            mimeType: 'text/plain'
                        }
                    ]
                }
            }
            throw new Error("No rows returned!");
        }
        catch (err: any) {
            console.error("Error occured: ", err.message);
            (await conn)?.end();
            return {
                contents: [
                    {
                        uri: uri.href,
                        text: "Could not retrieve databases! Service down!",
                        mimeType: 'text/plain'

                    }
                ]
            };
        }              
    }
);

// Retrieve database schema
server.registerResource(
    "db-schema",
    new ResourceTemplate(
        "internal://schemas/db-schema/{db}", { list: undefined }
    ),
    {
        title: 'Retrieve schema of a database',
        description: `
            Retrieve schema of a single database. Use this resource to list all tables and it's fields of a database
            Before creating a query, you MUST call all-databases resource and this one in order to get complete overview of databases, tables and their fields
        `,
        mimeType: 'application/json'
    },
    async (uri, {db}) => {
        const query = `
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY, EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = ?
            ORDER BY TABLE_NAME, ORDINAL_POSITION;
        `;

        let conn: mysql.Connection | undefined;

        try {
            conn = await mysql.createConnection(viewerConnectionDetails);
            const [rows] = await conn.execute<RowDataPacket[]>(query, [db]);
            await conn.end();
            if (rows.length > 0) {
                return {
                    contents: [
                        {
                            uri: uri.href,
                            text: JSON.stringify(rows),
                            mimeType: 'application/json'
                        }
                    ]
                }
            }
            throw new Error("Invalid database name!");
        }
        catch (err: any) {
            console.error(`Unexpected error: ${err.message}`);
            (await conn)?.end();
            return {
                contents: [
                    {
                        uri: uri.href,
                        text: err.message,
                        mimeType: 'text/plain'
                    }
                ]
            }
        }
    }
);

// Retrieve schemas across all databases
server.registerTool(
    'databases-search',
    {
        title: 'Search for schemas across databases',
        description: `
            Use this tool to search for schemas across all available databases in the system.
            Other tools require you to select a database and perform operations. This does not.
            It provides read-only access to all databases. You DO NOT have rights to edit data in this tool.
        `,
        inputSchema: {
            searchQuery: z.string().describe('MySQL supported SQL query to search for schema across all databases')
        }
    },
    async ({ searchQuery }) => {
        let conn: undefined | mysql.Connection;
        try {
            conn = await mysql.createConnection(viewerConnectionDetails);
            const [rows] = await conn.execute<RowDataPacket[]>(searchQuery);
            await conn.end();

            if (rows.length > 0) {
                return {
                    content: [
                        {
                            type: "text",
                            text: "Here is the output of the read-only query executed: "
                        },
                        {
                            type: "text",
                            text: JSON.stringify(rows)
                        }
                    ]
                }
            }
            return {
                content: [
                    {
                        type: "text",
                        text: "Executed query did not return any rows"
                    }
                ]
            }
        }
        catch (err: any) {
            console.error(err.message);
            (await conn)?.end();
            return {
                content: [
                    {
                        type: "text",
                        text: "Query did not get executed! Unexpected error occured!"
                    }
                ]
            }
        }
    }
);

// Executes inter-database read-only queries
server.registerTool(
    'execute-read-queries',
    {
        title: 'Execute Read-Only Queries',
        description: `
            Use this tool to execute queries that DOES NOT change the state of database (read only)
            This tool is used for inter-database operations (operations within a single database)
            The input must be of two lines:
                MySQL SQL command for selecting a database
                MySQL SQL query in string format
        `,
        inputSchema: {
            dbselect: z.string().describe("MySQL-supported SQL query to select a database"),
            query: z.string().describe("The read-only MySQL-supported SQL query")
        }
    },
    async ({ dbselect, query }) => {
        let conn: undefined | mysql.Connection;
        try {
            conn = await mysql.createConnection(viewerConnectionDetails);
            await conn.query(dbselect);
            const [rows] = await conn.execute<RowDataPacket[]>(query);
            await conn.end();

            if (rows.length > 0) {
                return {
                    content: [
                        {
                            type: "text",
                            text: "Here is the output of the read-only query executed: "
                        },
                        {
                            type: "text",
                            text: JSON.stringify(rows)
                        }
                    ]
                }
            }
            return {
                content: [
                    {
                        type: "text",
                        text: "Executed query did not return any rows"
                    }
                ]
            }
        }
        catch (err: any) {
            console.error(err.message);
            (await conn)?.end();
            return {
                content: [
                    {
                        type: "text",
                        text: "Query did not get executed! Unexpected error occured!"
                    }
                ]
            }
        }
    }
);

// Executes inter-database write/read-write queries
server.registerTool(
    'execute-write-queries',
    {
        title: "Execute read-write/write queries",
        description: `
            Choose this tool if the command generated WILL change the contents of the database.
            This tool is used for inter-database operations (operations within a single database)
            The input must be of three lines:
                MySQL SQL command for selecting a database
                MySQL SQL query command in string format
                MySQL SQL query that displays the changed data
            Do not include any transaction commands. They will be auto-inserted before the query command you generated.
            After user reviews and accepts/denies the result of the command, you can commit/rollback the transaction 
        `,
        inputSchema: {
            dbselect: z.string().describe("MySQL-supported SQL query to select a database"),
            queryCommand: z.string().describe("The generated MySQL SQL query command that will change the content of databases"),
            viewResults: z.string().describe("The generated MySQL SQL query that displays the changed data for confirmation")
        }
    },
    async ({dbselect, queryCommand, viewResults}) => {
        let conn: mysql.Connection | undefined;

        try {
            conn = await mysql.createConnection(editorConnectionDetails);

            await conn.query(dbselect);
            await conn.query('START TRANSACTION;')
            const [result] = await conn.execute<ResultSetHeader>(queryCommand);
            const [rows] = await conn.execute<RowDataPacket[]>(viewResults);

            await fs.writeFile('D:/Code/NL_to_SQL/summary.txt', JSON.stringify(rows));

            const confirmance = await server.server.elicitInput({
                mode: 'form',
                message: `The summary of operation: ${result.info}. Detailed summary is logged to D:/Code/NL_to_SQL/summary.txt`,
                requestedSchema: {
                    type: 'object',
                    properties: {
                        accepted: {
                            type: 'boolean',
                            title: 'Accept?',
                            description: "Accept if the query command correctly did what you expected, else deny to rollback changes"
                        }
                    }
                },
                required: ['accepted']
            });

            if (confirmance.action === 'accept' && confirmance.content?.accepted === true) {
                await conn.execute('COMMIT;');
                await conn.end();
                return {
                    content: [
                        {
                            type: 'text',
                            text: 'Updated database successfully'
                        }
                    ]
                }
            }

            await conn.execute('ROLLBACK;');
            await conn.end();
            return {
                content: [
                    {
                        type: 'text',
                        text: 'User cancelled the operation! Transaction is rolled back.'
                    }
                ]
            }
        }
        catch (err: any) {
            console.error(`Error occured: ${err.message}`);
            (await conn)?.execute('ROLLBACK;');
            (await conn)?.end();

            return {
                content: [
                    {
                        type: 'text',
                        text: 'Unexpected error occured!'
                    }
                ]
            };
        }
    }
);

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("DBMan MCP Server running on stdio");
}

main().catch((error) => {
  console.error("Fatal error in main():", error);
  process.exit(1);
});