import { DatabaseType } from "../../voodoo-shared/ISchema";

export function stringAsSql(dbtype: DatabaseType, str: string) {
    if (dbtype === "SQL Server") {
        if (!str)
            return "null"
        else if (typeof str !== "string")
            throw new Error("stringAsSql():  'str' is not a string");
        else
            return "'" + str.replace(/./g, function (char: string): string {
                switch (char) {
                    case "'":
                        return "''";
                    default:
                        return char;
                }
            }) + "'";

    }
    else
        throw new Error("todo: stringAsSql() dbtype: " + dbtype);

}