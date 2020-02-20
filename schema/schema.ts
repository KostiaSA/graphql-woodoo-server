import { ISchema, ITable, IDatabase, IColumn } from "../../voodoo-shared/ISchema";
import { GraphQLSchema, GraphQLObjectType, GraphQLObjectTypeConfig, Thunk, isType } from "graphql";
var fs = require('fs');

export class Schema {
    info: ISchema;

    tableByDbAndTableName: { [db_and_tables_name: string]: ITable };

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
        this.tableByDbAndTableName = {};
        for (let t of this.info.tables) {
            let key = t.dbname + ":" + t.name;
            if (!this.tableByDbAndTableName[key])
                this.tableByDbAndTableName[key] = t;
            else
                throw new Error(`Schema.createCache(): duplicate table '${key}'`);
        }

    }

    getTable(dbName: string, tableName: string): ITable {
        let key = dbName + ":" + tableName;
        let ret = this.tableByDbAndTableName[key];
        if (!ret)
            throw new Error(`Schema.getTable(): table '${tableName}' not found in database '${dbName}' `);
        return
    }

    getTableDatabase(table: ITable): IDatabase {

        let ret = this.info.databases.find((db) => db.name == table.dbname);
        if (!ret)
            throw new Error(`Schema.getTableDatabase(): table '${table.name}' not found in database '${table.dbname}' `);
        return ret;
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

}

export var schema: Schema = new Schema();

