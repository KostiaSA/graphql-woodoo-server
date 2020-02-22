import { ISchema, ITable, IDatabase, IColumn, DatabaseType, GraphqlType } from "../../voodoo-shared/ISchema";
import { GraphQLSchema, GraphQLObjectType, GraphQLObjectTypeConfig, Thunk, isType, GraphQLResolveInfo } from "graphql";
import { Args, SqlResolver } from "../resolver/SqlResolver";
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
                    type: "mssql",
                    connection: {
                        // host: "dark\\sql2012",
                        // username: "sa",
                        // password: "",
                        // database: "BuhtaWmsWeb2020",
                        host: "localhost",
                        username: "sa2",
                        password: "sonyk",
                        database: "woodoo",
                    }
                }
            ],
            tables: [{
                dbname: "бухта-wms",
                dbo: "dbo",
                name: "Сотрудник",
                object_alias: "sotdrudnik",
                array_alias: "sotdrudniki",
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
                columns: [
                    {
                        name: "Ключ",
                        alias: "id",
                        type: "IntValue",
                        sql_type: "VarChar",
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
        this.saveToJson();
        this.createCache();

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
        if (db.type == "mssql") {
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
        if (db.type == "mssql")
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
                database: db.connection.database,
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

        if (dbtype == "mssql")
            return "[" + sql_identifier.replace(/]/g, "]]") + "]";
        else
            throw new Error("todo: stringAsSql dbtype == mssql");

    }

    stringAsSql(dbtype: DatabaseType, str: string): string {
        if (typeof str != "string")
            throw new Error("stringAsSql(): parameter 'str' is not a string");

        if (dbtype == "mssql")
            return "'" + str.replace(/'/g, "''") + "'";
        else
            throw new Error("todo: stringAsSql dbtype == mssql");

    }

    numberAsSql(dbtype: DatabaseType, value: number): string {
        if (typeof value != "number")
            throw new Error("stringAsSql(): parameter 'value' is not a number");

        if (dbtype == "mssql")
            return value.toString();
        else
            throw new Error("todo: numberAsSql dbtype == mssql");

    }
}

//export var schema: Schema = new Schema();

