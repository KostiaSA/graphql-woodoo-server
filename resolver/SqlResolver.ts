import { graphql, buildSchema, parse, GraphQLResolveInfo, SelectionNode, FieldNode } from 'graphql';
import { SqlSelectBuilder } from './SqlBuilder';
import { Schema } from '../schema/schema';

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

    async resolve_query(): Promise<any> {
        let tableAlias = this.info.fieldName;
        let table = this.schema.getTableByArrayAlias(tableAlias);
        let database = this.schema.getTableDatabase(table);

        this.sql.from.addLine(table.name + " AS " + tableAlias);

        for (let field of this.info.fieldNodes[0].selectionSet.selections) {
            if (field.kind == 'Field') {
                let f = field as FieldNode;
                // let f_alias = f.name.value;
                // if (f.alias && f.alias.value)
                //     f_alias = f.alias.value;
                if (f.name.kind == "Name") {
                    let col = this.schema.getTableColumnByAlias(table, f.name.value);
                    this.sql.fields.add(col.name + " AS " + f.name.value);

                    for (let arg of f.arguments) {
                        if (arg.kind != "Argument")
                            throw "internal argument error";
                        if (arg.name.value == "where_eq") {
                            let valueAsSql: string;
                            if (arg.value.kind == "StringValue") {
                                valueAsSql = this.schema.stringAsSql(database.type, arg.value.value);
                            }
                            else if (arg.value.kind == "IntValue" || arg.value.kind == "FloatValue") {
                                valueAsSql = this.schema.numberAsSql(database.type, Number.parseFloat(arg.value.value));
                            }
                            else
                                throw new Error("todo: arg.value.kind==StringValue");

                            this.sql.where.add(col.name + "=" + valueAsSql);
                        }
                        else
                            throw new Error("todo: where_eq");
                    }
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