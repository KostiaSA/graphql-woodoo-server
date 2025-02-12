import { graphql, buildSchema, parse, GraphQLResolveInfo, SelectionNode, FieldNode, ArgumentNode, ValueNode } from 'graphql';
import { SqlSelectBuilder } from './SqlBuilder';
import { Schema } from '../schema/schema';
import { DatabaseType, ITable, IColumn, IDatabase } from '../../voodoo-shared/ISchema';

export type Args = { [argName: string]: any };



export class SqlResolver {
    schema: Schema;
    args: Args;
    info: GraphQLResolveInfo;
    sql: SqlSelectBuilder = new SqlSelectBuilder();
    rowMapper: string[] = [];

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

    processFieldNode(f: SelectionNode, database: IDatabase, table: ITable, col: IColumn, fromAlias: string, levelFieldName: string) {

        if (f.kind == "Field") {
            if (f.name.kind != "Name")
                throw "processFieldNode(): internal error f.name.kind != 'Name'";


            if (col.ref_table) {
                let join_table = this.schema.getTable(col.ref_db, col.ref_schema, col.ref_table);
                let join_table_alias = this.schema.getTableObjectAlias(join_table);
                let join_database = this.schema.getTableDatabase(join_table);
                let join_fromAlias = fromAlias + "_" + join_table_alias + "_" + this.sql.from.sql.length;

                this.rowMapper.push(`${f.name.value}:{`);

                let joinStr = "LEFT JOIN " + this.schema.getTable3PartName(join_table) + " AS " + join_fromAlias;
                //identifierAsSql
                let joinOnColumnsList: string[] = col.ref_columns.map((item) => fromAlias + "." + this.schema.identifierAsSql(database.type, item.column) + " = " + join_fromAlias + "." + this.schema.identifierAsSql(database.type, item.ref_column));
                let joinOnStr = "ON " + joinOnColumnsList.join(" AND ");
                this.sql.from.add(joinStr);
                for (let ff of f.selectionSet.selections) {
                    if (ff.kind == "Field") {
                        let ff_col = this.schema.getTableColumnByAlias(join_table, ff.name.value);
                        this.processFieldNode(ff, join_database, join_table, ff_col, join_fromAlias, (levelFieldName || "") + f.name.value);
                    }
                    else
                        throw "processFieldNode(): todo: for ff.kind:" + ff.kind;
                }
                this.sql.from.add(joinOnStr);
                this.rowMapper.push(`},`);
            }
            else {
                let is_hidden = false;

                for (let arg of f.arguments) {
                    if (arg.kind != "Argument")
                        throw "internal argument error";
                    //if (arg.name.value.startsWith("where_")) {    

                    if (this.schema.where_opers[arg.name.value]) {
                        let where_oper = this.schema.where_opers[arg.name.value];

                        let p0 = fromAlias + "." + this.schema.identifierAsSql(database.type, col.name);
                        let p1: string;

                        if (where_oper.sql_string.indexOf("%1") > -1) {
                            p1 = this.argValueToSql(database.type, arg.value, arg.name.value.indexOf("between") > -1);
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

                if (!is_hidden) {

                    //num: row.num_0,
                    let field_alias: string;
                    if (levelFieldName)
                        field_alias = levelFieldName + "_" + f.name.value + "_" + this.sql.fields.sql.length;
                    else
                        field_alias = f.name.value + "_" + this.sql.fields.sql.length;

                    if (col.sql_type == "Date" || col.sql_type == "SmallDateTime")
                        this.rowMapper.push(`${f.name.value}:row.${field_alias}.toISOString().substr(0,10),`);
                    else if (col.sql_type == "DateTime" || col.sql_type == "DateTime2" || col.sql_type == "DateTimeOffset")
                        this.rowMapper.push(`${f.name.value}:row.${field_alias}.toISOString(),`);
                    else
                        this.rowMapper.push(`${f.name.value}:row.${field_alias},`);

                    this.sql.fields.add(fromAlias + "." + this.schema.identifierAsSql(database.type, col.name) + " AS " + field_alias);
                }

            }
        }
        else
            throw "processFieldNode(): todo: for f.kind:" + f.kind;
    }

    async resolve_query(): Promise<any> {
        console.time("=============resolver====================");
        let tableAlias = this.info.fieldName;
        let table = this.schema.getTableByArrayAlias(tableAlias);
        let database = this.schema.getTableDatabase(table);
        let rootFromAlias = tableAlias;

        this.rowMapper.push("return function (row) { return {");

        this.sql.from.addLine(this.schema.getTable3PartName(table) + " AS " + rootFromAlias);

        for (let field of this.info.fieldNodes[0].selectionSet.selections) {
            if (field.kind == 'Field') {
                let col = this.schema.getTableColumnByAlias(table, field.name.value);
                this.processFieldNode(field, database, table, col, rootFromAlias, "");
            }
            else
                throw "resolve_query(): todo: for field.kind:" + field.kind;
        }
        this.rowMapper.push("}}");


        console.timeEnd("=============resolver====================");
        console.log(this.sql.toSql());
        //console.log("this.sql.toSql()");

        let execute_result: any;
        //for (let i = 0; i < 100; i++) {
        console.time("=============sqlExecute====================");
        execute_result = await this.schema.sqlExecute("бухта-wms", this.sql.toSql());
        console.timeEnd("=============sqlExecute====================");
        //}

        console.time("=============new mapper====================");
        let mapperFunc = new Function(this.rowMapper.join("\n"))();
        console.timeEnd("=============new mapper====================");

        //console.log(execute_result.recordset);//.map( this.rowMapper));
        //console.log(execute_result.recordset.map(mapperFunc));
        //console.log(this.rowMapper.join("\n"));

        console.time("=============map====================");
        let result = execute_result.recordset.map(mapperFunc);
        console.timeEnd("=============map====================");
        return result;
    }
}