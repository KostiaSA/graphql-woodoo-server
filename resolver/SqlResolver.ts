import { graphql, buildSchema, parse, GraphQLResolveInfo, SelectionNode, FieldNode, ArgumentNode, ValueNode } from 'graphql';
import { SqlSelectBuilder } from './SqlBuilder';
import { Schema } from '../schema/schema';
import { DatabaseType } from '../../voodoo-shared/ISchema';

export type Args = { [argName: string]: any };



export class SqlResolver {
    schema: Schema;
    args: Args;
    info: GraphQLResolveInfo;
    sql: SqlSelectBuilder = new SqlSelectBuilder();

    constructor(_schema: Schema, _args: Args, _info: GraphQLResolveInfo) {
        this.schema = _schema;
        this.args = _args;
        this.info = _info;
    }

    async resolve(): Promise<any> {

        if (this.info.operation.operation == "query") {
            return this.resolve_query();
        }
        else
            throw "todo:"

    }

    argValueToSql(dbype: DatabaseType, arg_value: ValueNode, betweenMode: boolean = false): string {
        if (arg_value.kind == "StringValue") {
            return this.schema.stringAsSql(dbype, arg_value.value);
        }
        else if (arg_value.kind == "IntValue" || arg_value.kind == "FloatValue") {
            return this.schema.numberAsSql(dbype, Number.parseFloat(arg_value.value));
        }
        else if (arg_value.kind == "ListValue") {
            if (betweenMode) {
                if (arg_value.values.length != 2) {
                    throw new Error("BETWEEEN requires 2 arguments");
                }
                return arg_value.values.map((val) => this.argValueToSql(dbype, val, betweenMode)).join(" AND ");
            }
            else {
                if (arg_value.values.length == 0) {
                    throw new Error("requires at least 1 argument");
                }
                return arg_value.values.map((val) => this.argValueToSql(dbype, val, betweenMode)).join(",");
            }
        }
        else {
            debugger
            throw new Error("todo: arg.value.kind==StringValue");
        }

    }

    async resolve_query(): Promise<any> {
        let tableAlias = this.info.fieldName;
        let table = this.schema.getTableByArrayAlias(tableAlias);
        let database = this.schema.getTableDatabase(table);
        let rootFromAlias = tableAlias;


        this.sql.from.addLine(this.schema.identifierAsSql(database.type, table.name) + " AS " + rootFromAlias);

        for (let field of this.info.fieldNodes[0].selectionSet.selections) {
            if (field.kind == 'Field') {
                let f = field as FieldNode;
                // let f_alias = f.name.value;
                // if (f.alias && f.alias.value)
                //     f_alias = f.alias.value;
                if (f.name.kind == "Name") {
                    let col = this.schema.getTableColumnByAlias(table, f.name.value);
                    let is_hidden = false;

                    for (let arg of f.arguments) {
                        if (arg.kind != "Argument")
                            throw "internal argument error";
                        //if (arg.name.value.startsWith("where_")) {    

                        if (this.schema.where_opers[arg.name.value]) {
                            let where_oper = this.schema.where_opers[arg.name.value];

                            let p0 = rootFromAlias + "." + this.schema.identifierAsSql(database.type, col.name);
                            let p1: string;

                            if (where_oper.sql_string.indexOf("%1") > -1) {
                                p1 = this.argValueToSql(database.type, arg.value, arg.name.value.indexOf("between") > -1);
                                // if (arg.value.kind == "StringValue") {
                                //     p1 = this.schema.stringAsSql(database.type, arg.value.value);
                                // }
                                // else if (arg.value.kind == "IntValue" || arg.value.kind == "FloatValue") {
                                //     p1 = this.schema.numberAsSql(database.type, Number.parseFloat(arg.value.value));
                                // }
                                // else {
                                //     debugger
                                //     throw new Error("todo: arg.value.kind==StringValue");
                                // }
                            }

                            let whereStr = where_oper.sql_string.replace("%1", p1 || "").replace("%0", p0 || "");

                            this.sql.where.add(whereStr);
                        }
                        else if (arg.name.value == "is_hidden") {
                            is_hidden = true;
                        }
                        else
                            throw new Error("todo: unknown where_eq param: " + arg.name.value);

                    }

                    if (!is_hidden)
                        this.sql.fields.add(rootFromAlias + "." + this.schema.identifierAsSql(database.type, col.name) + " AS " + f.name.value);
                }
                else
                    throw new Error("todo: f.name.kind == Name");
            }
            else
                throw new Error("todo: field.kind == 'Field'");
        }

        console.log("this.sql.toSql()");
        console.log(this.sql.toSql());
        let execute_result = await this.schema.sqlExecute("бухта-wms", this.sql.toSql());
        console.log(execute_result.recordset);
        return execute_result.recordset;
    }
}