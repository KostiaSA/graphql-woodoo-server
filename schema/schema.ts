import { ISchema, ITable, IDatabase, IColumn, DatabaseType, GraphqlType, IDatabaseConnection } from "../../voodoo-shared/ISchema";
import { GraphQLSchema, GraphQLObjectType, GraphQLObjectTypeConfig, Thunk, isType, GraphQLResolveInfo } from "graphql";
import { Args, SqlResolver } from "../resolver/SqlResolver";
import { stringAsSql } from "../utils/stringAsSql";
var fs = require('fs');
var mssql = require('mssql')

export interface IWhereOper {
    data_types?: GraphqlType[];
    all_types?: boolean;
    sql_string: string;
    p1_is_array?: boolean;
}

export class Schema {
    info: ISchema;

    tableByDbAndTableName: { [db_and_tables_name: string]: ITable };
    tableByObjectAlias: { [table_array_alias: string]: ITable };
    tableByArrayAlias: { [table_array_alias: string]: ITable };
    databaseByName: { [db_name: string]: IDatabase };
    mssql_pool: { [db_name: string]: any };
    tableColumnByAlias: { [table_name_and_col_alias: string]: IColumn };

    where_opers: { [where_param_name: string]: IWhereOper } = {
        where_eq: {
            all_types: true,
            data_types: ["IntValue", "FloatValue", "StringValue"],
            sql_string: "%0=%1",
        },
        where_not_eq: {
            data_types: ["IntValue", "FloatValue", "StringValue"],
            sql_string: "%0<>%1",
        },
        where_between: {
            data_types: ["IntValue", "FloatValue", "StringValue"],
            sql_string: "%0 BETWEEN %1",
            p1_is_array: true
        },
        where_gt: {
            data_types: ["IntValue", "FloatValue", "StringValue"],
            sql_string: "%0>%1",
        },
        where_gte: {
            data_types: ["IntValue", "FloatValue", "StringValue"],
            sql_string: "%0>=%1",
        },
        where_lt: {
            data_types: ["IntValue", "FloatValue", "StringValue"],
            sql_string: "%0<%1",
        },
        where_lte: {
            data_types: ["IntValue", "FloatValue", "StringValue"],
            sql_string: "%0<=%1",
        },
        where_is_null: {
            all_types: true,
            sql_string: "%0 IS NULL",
        },
        where_is_not_null: {
            all_types: true,
            sql_string: "%0 IS NOT NULL",
        },
        where_in: {
            data_types: ["IntValue", "FloatValue", "StringValue"],
            sql_string: "%0 IN (%1)",
            p1_is_array: true
        },
        where_like: {
            data_types: ["StringValue"],
            sql_string: "%0 LIKE %1",
        },
        where_not_like: {
            data_types: ["StringValue"],
            sql_string: "%0 NOT LIKE %1",
        },
    }

    constructor() {
        console.log("Schema constr");
        this.info = {
            databases: [
                {
                    name: "бухта-wms",
                    prefix: "wms",
                    description: "это база учета wms",
                    type: "SQL Server",
                    version: 1,
                    connection: {
                        // host: "dark\\sql2012",
                        // username: "sa",
                        // password: "",
                        // database: "BuhtaWmsWeb2020",
                        host: "localhost",
                        port: 1433,
                        username: "sa2",
                        password: "sonyk",
                        database: "woodoo",
                    }
                },
                {
                    name: "бухта-erp",
                    prefix: "erp",
                    description: "это резервная копия",
                    type: "SQL Server",
                    version: 1,
                    connection: {
                        // host: "dark\\sql2012",
                        // username: "sa",
                        // password: "",
                        // database: "BuhtaWmsWeb2020",
                        host: "localhost",
                        port: 1433,
                        username: "sa2",
                        password: "sonyk",
                        database: "woodoo",
                    }
                },

            ],
            tables: [{
                dbname: "бухта-wms",
                dbo: "dbo",
                name: "Сотрудник",
                object_alias: "sotdrudnik",
                array_alias: "sotdrudniki",
                version: 1,
                columns: [
                    {
                        name: "Номер",
                        alias: "num",
                        type: "StringValue",
                        sql_type: "VarChar",
                    },
                    {
                        name: "Фамилия",
                        alias: "fa",
                        type: "StringValue",
                        sql_type: "VarChar",
                    },
                    {
                        name: "Имя",
                        alias: "im",
                        type: "StringValue",
                        sql_type: "VarChar",
                    },
                    {
                        name: "ЗП Подразделение",
                        alias: "podr_id",
                        type: "IntValue",
                        sql_type: "Int",
                    },
                    {
                        name: "podr",
                        type: "ObjectValue",
                        ref_db: "бухта-wms",
                        ref_table: "Подразделение",
                        ref_columns: [{
                            column: "ЗП Подразделение", ref_column: "Ключ"
                        }]
                    }
                ]
            },
            {
                dbname: "бухта-wms",
                dbo: "dbo",
                name: "Подразделение",
                object_alias: "podr",
                array_alias: "podrs",
                version: 1,
                columns: [
                    {
                        name: "Ключ",
                        alias: "id",
                        type: "IntValue",
                        sql_type: "Int",
                    },
                    {
                        name: "Номер",
                        alias: "num",
                        type: "StringValue",
                        sql_type: "VarChar",
                    },
                    {
                        name: "Название",
                        alias: "name",
                        type: "StringValue",
                        sql_type: "VarChar",
                    },
                ]
            },
            {
                dbname: "бухта-wms",
                dbo: "dbo",
                name: "ТМЦ",
                object_alias: "tmc",
                array_alias: "tmcs",
                version: 1,
                columns: [
                    {
                        name: "Ключ",
                        alias: "id",
                        type: "IntValue",
                        sql_type: "Int",
                    },
                    {
                        name: "Номер",
                        alias: "num",
                        type: "StringValue",
                        sql_type: "VarChar",
                    },
                    {
                        name: "Название",
                        alias: "name",
                        type: "StringValue",
                        sql_type: "VarChar",
                    },
                    {
                        name: "Вид",
                        alias: "vid_id",
                        type: "IntValue",
                        sql_type: "Int",
                    },
                    {
                        name: "vid",
                        type: "ObjectValue",
                        ref_db: "бухта-wms",
                        ref_table: "Вид ТМЦ",
                        ref_columns: [{
                            column: "Вид", ref_column: "Ключ"
                        }]
                    }
                ]
            },
            {
                dbname: "бухта-wms",
                dbo: "dbo",
                name: "Вид ТМЦ",
                object_alias: "tmcvid",
                array_alias: "tmcvids",
                version: 1,
                columns: [
                    {
                        name: "Ключ",
                        alias: "id",
                        type: "IntValue",
                        sql_type: "VarChar",
                    },
                    {
                        name: "Номер",
                        alias: "num",
                        type: "StringValue",
                        sql_type: "VarChar",
                    },
                    {
                        name: "Название",
                        alias: "name",
                        type: "StringValue",
                        sql_type: "VarChar",
                    },
                ]
            },
            {
                dbname: "бухта-wms",
                dbo: "dbo",
                name: "Проводка",
                object_alias: "prov",
                array_alias: "provs",
                version: 1,
                columns: [
                    {
                        name: "_id",
                        alias: "_id",
                        type: "StringValue",
                        sql_type: "UniqueIdentifier",
                    },
                    {
                        name: "Ключ",
                        alias: "id",
                        type: "IntValue",
                        sql_type: "VarChar",
                    },
                    {
                        name: "Дата",
                        alias: "date",
                        type: "StringValue",
                        sql_type: "Date",
                    },
                    {
                        name: "Сумма",
                        alias: "summa",
                        type: "FloatValue",
                        sql_type: "Money",
                    },
                    {
                        name: "Дб субконто 2",
                        alias: "db_subconto2",
                        type: "IntValue",
                        sql_type: "Int",
                    },
                    {
                        name: "Дебет",
                        alias: "debet",
                        type: "StringValue",
                        sql_type: "VarChar",
                    },
                    {
                        name: "tmc",
                        type: "ObjectValue",
                        ref_db: "бухта-wms",
                        ref_table: "ТМЦ",
                        ref_columns: [{
                            column: "Дб субконто 2", ref_column: "Ключ"
                        }]
                    },
                    {
                        name: "Кредит",
                        alias: "kredit",
                        type: "StringValue",
                        sql_type: "VarChar",
                    },
                ]
            },
            ]
        }
        this.loadFromToJson();
        this.saveToJson();
        this.createCache();

    }

    loadFromToJson() {
        if (fs.existsSync('voodoo-schema.json')) {
            this.info = JSON.parse(fs.readFileSync('voodoo-schema.json'));
        }
    }

    saveToJson() {
        fs.writeFileSync('voodoo-schema.json', JSON.stringify(this.info), { encoding: "utf8" });
    }

    createCache() {
        this.databaseByName = {};
        for (let d of this.info.databases) {
            let key = d.name;
            if (!this.databaseByName[key])
                this.databaseByName[key] = d;
            else
                throw new Error(`Schema.createCache(): duplicate database '${key}'`);
        }

        this.tableByDbAndTableName = {};
        for (let t of this.info.tables) {
            let key = t.dbname + ":" + t.name;
            if (!this.tableByDbAndTableName[key])
                this.tableByDbAndTableName[key] = t;
            else
                throw new Error(`Schema.createCache(): duplicate table '${key}'`);
        }

        this.tableByArrayAlias = {};
        for (let t of this.info.tables) {
            let key = this.getTableArrayAlias(t);
            if (!this.tableByArrayAlias[key])
                this.tableByArrayAlias[key] = t;
            else
                throw new Error(`Schema.createCache(): duplicate table array-alias '${key}'`);
        }

        this.tableByObjectAlias = {};
        for (let t of this.info.tables) {
            let key = this.getTableArrayAlias(t);
            if (!this.tableByObjectAlias[key])
                this.tableByObjectAlias[key] = t;
            else
                throw new Error(`Schema.createCache(): duplicate table object-alias '${key}'`);
        }

        this.tableColumnByAlias = {};
        for (let t of this.info.tables) {
            for (let c of t.columns) {
                let key = t.name + ":" + this.getTableColAlias(c);
                if (!this.tableColumnByAlias[key])
                    this.tableColumnByAlias[key] = c;
                else
                    throw new Error(`Schema.createCache(): duplicate table-name + column-alias '${key}'`);
            }
        }
    }

    getDatabase(dbName: string): IDatabase {
        let key = dbName;
        let ret = this.databaseByName[key];
        if (!ret)
            throw new Error(`Schema.getDatabase(): table '${key}' not found`);
        return ret;
    }

    getTable3PartName(table: ITable) {
        let db = this.getDatabase(table.dbname);
        if (db.type == "SQL Server") {
            return `[${db.connection.database}].[${table.dbo}].[${table.name}]`
        }
        else
            throw new Error(`getTable4PartName(): todo: for ` + db.type);

    }

    getTable(dbName: string, tableName: string): ITable {
        let key = dbName + ":" + tableName;
        let ret = this.tableByDbAndTableName[key];
        if (!ret)
            throw new Error(`Schema.getTable(): table '${tableName}' not found in database '${dbName}' `);
        return ret;
    }

    getTableByArrayAlias(alias: string): ITable {
        let ret = this.tableByArrayAlias[alias];
        if (!ret)
            throw new Error(`Schema.getTableByArrayAlias(): table by array-alias '${alias}' not found`);
        return ret;
    }

    getTableByObjectAlias(alias: string): ITable {
        let ret = this.tableByObjectAlias[alias];
        if (!ret)
            throw new Error(`Schema.getTableByObjectAlias(): table by object-alias '${alias}' not found`);
        return ret;
    }

    getTableColumnByAlias(table: ITable, colAlias: string): IColumn {
        let key = table.name + ":" + colAlias;
        let ret = this.tableColumnByAlias[key];
        if (!ret)
            throw new Error(`Schema.getTableColumnByAlias(): table column '${key}' not found`);
        return ret;
    }

    getTableDatabase(table: ITable): IDatabase {
        return this.getDatabase(table.dbname);
    }

    getTableObjectAlias(table: ITable) {

        let prefix = this.getTableDatabase(table).prefix;
        if (prefix)
            return prefix + "_" + (table.object_alias || table.name);
        else
            return table.object_alias || table.name;
    }

    getTableArrayAlias(table: ITable) {

        let prefix = this.getTableDatabase(table).prefix;
        if (prefix)
            return prefix + "_" + (table.array_alias || table.name);
        else
            return table.array_alias || table.name;
    }

    getTableColAlias(tableCol: IColumn) {
        return tableCol.alias || tableCol.name;
    }

    createGraphQLResolvers(): any {
        let ret: any = {
            Query: {
            },
        };

        for (let table of this.info.tables) {
            if (table.disabled)
                continue;

            ret.Query[this.getTableArrayAlias(table)] = (parent: any, args: Args, context: any, info: GraphQLResolveInfo) => {
                console.log("ret.Query[this.getTableObjectAlias(table)] ");
                let resolver = new SqlResolver(this, args, info);
                return resolver.resolve();
            };
        }

        return ret;

    }

    createGraphQLSchema(): string {

        let defStr: string[] = [];
        let queryStr: string[] = [];

        for (let table of this.info.tables) {
            if (table.disabled)
                continue;
            let fields: string[] = [];
            for (let col of table.columns) {

                if (col.type == "ObjectValue") {  // fk
                    if (col.ref_table) {
                        let refTable = this.getTable(col.ref_db, col.ref_table);

                        fields.push(`${this.getTableColAlias(col)}:${this.getTableObjectAlias(refTable)}`);

                    }
                    else
                        throw new Error("todo: ObjectList");
                }
                else {
                    let typeStr = col.type.replace("Value", "");

                    let where_params: string[] = [];
                    for (let where_oper_name in this.where_opers) {
                        let where_oper = this.where_opers[where_oper_name];
                        if (where_oper.all_types || where_oper.data_types.indexOf(col.type) > -1) {
                            if (where_oper_name == "where_is_null" || where_oper_name == "where_is_not_null")
                                where_params.push(where_oper_name + ":Boolean");
                            else if (where_oper.p1_is_array)
                                where_params.push(where_oper_name + ":[" + typeStr + "]");
                            else
                                where_params.push(where_oper_name + ":" + typeStr);
                        }

                    }

                    where_params.push("is_hidden:Boolean");

                    if (where_params.length == 0)
                        fields.push(`${this.getTableColAlias(col)}:${typeStr}`);
                    else
                        fields.push(`${this.getTableColAlias(col)}(${where_params.join(",")}):${typeStr}`);
                }
            }

            defStr.push(`type ${this.getTableObjectAlias(table)} {${fields.join(" ")}}`);
            queryStr.push(`${this.getTableObjectAlias(table)}: ${this.getTableObjectAlias(table)}`);

            queryStr.push(`${this.getTableArrayAlias(table)}: [${this.getTableObjectAlias(table)}]`);
        }

        return defStr.join(" ") + ` type Query {${queryStr.join(" ")}}`;
        // let query_config: any = {
        //     name: 'Query',
        //     fields: {}
        // };
        // //       const AddressType = new GraphQLObjectType({
        // //             name: 'Address',
        // //             fields: {
        // //   street: { type: GraphQLString },
        // //               number: { type: GraphQLInt },
        // //               formatted: {
        // //     type: GraphQLString,
        // //                 resolve(obj) {
        // //       return obj.number + ' ' + obj.street
        // //                         }
        // //   }
        // //             }
        // //  });


        // for (let table of this.info.tables) {
        //     let tableObjectType: any = new GraphQLObjectType({
        //         name: table.name,
        //         fields: {
        //             id: { type: graphql.GraphQLString },
        //             name: { type: graphql.GraphQLString },
        //         }
        //     });

        //     query_config.fields[table.object_alias || table.name] = {
        //         type: tableObjectType
        //     };
        // }

        // let query_type: GraphQLObjectType = new GraphQLObjectType(query_config);


        // return new GraphQLSchema({ query:});
    }


    async sqlExecute(dbName: string, sql: string): Promise<any> {
        let db = this.getDatabase(dbName);
        if (db.type == "SQL Server")
            return this.sqlExecute_mssql(db, sql);
        else
            throw new Error(`sqlExecute(): todo:`);
    }

    async sqlExecute_mssql(db: IDatabase, sql: string): Promise<any> {
        this.mssql_pool = this.mssql_pool || {};
        let pool = this.mssql_pool[db.name];
        if (!pool) {
            let config = {
                user: db.connection.username,
                password: db.connection.password,
                server: db.connection.host,
                port: db.connection.port > 0 && db.connection.port != 1433 ? db.connection.port : undefined,
                database: db.connection.database,
                coonectionTimeout: 1000 * 15,
                requestTimeout: 1000 * 1800,
                pool: {
                    max: 100,
                    min: 10,
                    idleTimeoutMillis: 3600 * 1000
                }
            }
            pool = new mssql.ConnectionPool(config);
            await pool.connect();
            this.mssql_pool[db.name] = pool;
        }
        return pool.request().query(sql);
        //const pool = new sql.ConnectionPool(config)

    }

    getDatabaseType(dbname: string) {
        return this.getDatabase(dbname).type;
    }

    identifierAsSql(dbtype: DatabaseType, sql_identifier: string): string {
        if (typeof sql_identifier != "string")
            throw new Error("stringAsSql(): parameter 'sql_identifier' is not a string");

        if (dbtype == "SQL Server")
            return "[" + sql_identifier.replace(/]/g, "]]") + "]";
        else
            throw new Error("todo: stringAsSql dbtype: " + dbtype);

    }

    stringAsSql(dbtype: DatabaseType, str: string): string {
        if (typeof str != "string")
            throw new Error("stringAsSql(): parameter 'str' is not a string");

        if (dbtype == "SQL Server")
            return "'" + str.replace(/'/g, "''") + "'";
        else
            throw new Error("todo: stringAsSql dbtype: " + dbtype);

    }

    numberAsSql(dbtype: DatabaseType, value: number): string {
        if (typeof value != "number")
            throw new Error("stringAsSql(): parameter 'value' is not a number");

        if (dbtype == "SQL Server")
            return value.toString();
        else
            throw new Error("todo: numberAsSql dbtype: " + dbtype);

    }

    upsertDatabase(db: IDatabase) {
        let index = this.info.databases.findIndex((_db) => _db.name == db.name);
        if (index == -1) {
            this.info.databases.push(db);
            db.version = 1;
        }
        else {
            if (this.info.databases[index].version !== db.version)
                throw new Error("database was changed by another user");
            this.info.databases[index] = db;
            db.version += 1;
        }

        setTimeout(() => {
            this.saveToJson();
            this.createCache();
        }, 100);
    }

    deleteDatabase(db_name: string) {
        let index = this.info.databases.findIndex((_db) => _db.name == db_name);
        if (index == -1)
            throw new Error("database '" + db_name + "' not found to remove");
        else {
            this.info.databases.splice(index, 1);
            this.info.tables = this.info.tables.filter((table) => table.dbname != db_name);
        }

        setTimeout(() => {
            this.saveToJson();
            this.createCache();
        }, 100);
    }

    async checkDatabaseConnection(db_type: DatabaseType, connection: IDatabaseConnection): Promise<string> {

        if (db_type == "SQL Server") {
            try {
                let config = {
                    user: connection.username,
                    password: connection.password,
                    server: connection.host,
                    port: connection.port > 0 && connection.port != 1433 ? connection.port : undefined,
                    database: connection.database,
                    requestTimeout: 3000,
                    connectionTimeout: 3000,
                    pool: {
                        max: 10,
                        min: 1,
                        idleTimeoutMillis: 3600 * 1000
                    }
                }
                let pool = new mssql.ConnectionPool(config);
                await pool.connect();
                await pool.request().query("select 1");
                pool.close();
                return "Ok";
            }
            catch (err) {
                return err.toString();
            }
        }
        else
            throw new Error("todo: checkDatabaseConnection() dbtype: " + db_type);

    }

    async getDatabaseNativeTables(dbName: string): Promise<string[]> {
        let db = this.databaseByName[dbName];

        if (db.type == "SQL Server") {
            let execute_result = await this.sqlExecute_mssql(db, "select TABLE_SCHEMA,TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_TYPE='BASE TABLE' order by TABLE_SCHEMA,TABLE_NAME");
            let result = execute_result.recordset.map((row: any) => { return { schema_name: row.TABLE_SCHEMA, table_name: row.TABLE_NAME }; });
            return result;
        }
        else
            throw new Error("todo: getDatabaseNativeTables() dbtype: " + db.type);

    }

    async getDatabaseNativeTableColumns(dbName: string, table_schema: string, table_name: string): Promise<string[]> {
        let db = this.databaseByName[dbName];

        if (db.type === "SQL Server") {
            let sql = `
select COLUMN_NAME, DATA_TYPE 
from INFORMATION_SCHEMA.COLUMNS 
where 
  TABLE_SCHEMA=${stringAsSql(db.type, table_schema)} AND 
  TABLE_NAME=${stringAsSql(db.type, table_name)} order by ORDINAL_POSITION`;

            let execute_result = await this.sqlExecute_mssql(db, sql);
            let result = execute_result.recordset.map((row: any) => { return { name: row.COLUMN_NAME, type: row.DATA_TYPE }; });
            return result;
        }
        else
            throw new Error("todo: getDatabaseNativeTableColumns() dbtype: " + db.type);

    }

    upsertTable(table: ITable) {
        let index = this.info.tables.findIndex((_table) => _table.name == table.name && _table.dbo == table.dbo);
        if (index == -1) {
            table.version = 1;
            this.info.tables.push(table);
        }
        else {
            if (this.info.tables[index].version !== table.version)
                throw new Error("table was changed by another user");
            this.info.tables[index] = table;
            table.version += 1;
        }

        setTimeout(() => {
            this.saveToJson();
            this.createCache();
        }, 100);
    }

}

//export var schema: Schema = new Schema();

