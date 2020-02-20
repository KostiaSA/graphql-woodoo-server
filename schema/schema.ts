import { ISchema, ITable, IDatabase, IColumn } from "../../voodoo-shared/ISchema";
import { GraphQLSchema, GraphQLObjectType, GraphQLObjectTypeConfig, Thunk, isType, GraphQLResolveInfo } from "graphql";
import { Args, SqlResolver } from "../resolver/SqlResolver";
var fs = require('fs');
var mssql = require('mssql')

export class Schema {
    info: ISchema;

    tableByDbAndTableName: { [db_and_tables_name: string]: ITable };
    tableByObjectAlias: { [table_array_alias: string]: ITable };
    tableByArrayAlias: { [table_array_alias: string]: ITable };
    databaseByName: { [db_name: string]: IDatabase };
    mssql_pool: { [db_name: string]: any };
    tableColumnByAlias: { [table_name_and_col_alias: string]: IColumn };

    constructor() {
        console.log("Schema constr");
        this.info = {
            databases: [
                {
                    name: "бухта-wms",
                    prefix: "wms",
                    type: "mssql",
                    connection: {
                        host: "dark\\sql2012",
                        username: "sa",
                        password: "",
                        database: "BuhtaWmsWeb2020",
                    }
                }
            ],
            tables: [{
                dbname: "бухта-wms",
                name: "Сотрудник",
                object_alias: "sotdrudnik",
                array_alias: "sotdrudniki",
                columns: [
                    {
                        name: "Номер",
                        alias: "num",
                    },
                    {
                        name: "Фамилия",
                        alias: "fa",
                    },
                    {
                        name: "Имя",
                        alias: "im",
                    }
                ]
            }
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
                // podrs: async (parent: any, args: Args, context: any, info: GraphQLResolveInfo) => {

                //     let r = new SqlResolver(args, info);
                //     await r.resolve_query();

                //     // let xxx = c.fieldName;
                //     // let yyy = c.fieldNodes[0].selectionSet.selections.map((item: any) => item.name.value).join();
                //     // //console.log("select", yyy, "from", xxx);
                //     // debugger
                //     return [{ podr_number: "1", podr_name: "2222222222", vid: { podrvid_id: "xxx", podrvid_name: "podrvid_name111" } }];
                // }
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

                fields.push(`${this.getTableColAlias(col)}:String`);
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
                pool: {
                    max: 100,
                    min: 0,
                    idleTimeoutMillis: 30000
                }
            }
            pool = new mssql.ConnectionPool(config);
            await pool.connect();
        }
        return pool.request().query(sql);
        //const pool = new sql.ConnectionPool(config)

    }
}

//export var schema: Schema = new Schema();

