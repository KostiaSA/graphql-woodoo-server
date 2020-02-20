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
        debugger
        let tableAlias = this.info.fieldName;
        let table = this.schema.getTableByArrayAlias(tableAlias);

        this.sql.from.addLine(table.name + " AS " + tableAlias);

        for (let field of this.info.fieldNodes[0].selectionSet.selections) {
            if (field.kind == 'Field') {
                let f = field as FieldNode;
                if (f.name.kind == "Name") {
                    let colAlias = f.name.value;
                    let col = this.schema.getTableColumnByAlias(table, colAlias);
                    this.sql.fields.add(col.name + " AS " + f.name.value);
                }
                else
                    throw "todo:"
            }
            else
                throw "todo:"
        }

        console.log("this.sql.toSql()");
        console.log(this.sql.toSql());
        let execute_result = await this.schema.sqlExecute("бухта-wms", this.sql.toSql());
        console.log(execute_result.recordset);
        return execute_result.recordset;
    }
}