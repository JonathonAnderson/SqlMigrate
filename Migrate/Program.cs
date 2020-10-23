using System.Data;
using System.Data.Odbc;

namespace Migrate
{
    class Program
    {
        static void Main(string[] args)
        {

            string connectionStringSrc = "Driver={ODBC Driver 17 for SQL Server}; Server=192.168.2.124; Database=SOURCE; UID=test; PWD=password;";
            string connectionStringDst = "Driver={ODBC Driver 17 for SQL Server}; Server=192.168.2.124; Database=SOURCE; UID=test; PWD=password;";

            string queryStringSrc = "SELECT TOP (10) * FROM Sales.Orders";
            string queryStringDst = "SELECT * FROM Sales.Test";

            // DataSets hold results of SELECT queries
            DataSet dataSetSrc = new DataSet();
            DataSet dataSetDst = new DataSet();

            // Set up a connection to each database and open the connections
            using (OdbcConnection connectionDst = new OdbcConnection(connectionStringDst))
            {
                using (OdbcConnection connectionSrc = new OdbcConnection(connectionStringSrc))
                {
                    connectionSrc.Open();
                    connectionDst.Open();

                    // Adapters execute the query and fill the DataSet with the results
                    OdbcDataAdapter sourceAdapter = new OdbcDataAdapter(queryStringSrc, connectionSrc);
                    OdbcDataAdapter destAdapter = new OdbcDataAdapter(queryStringDst, connectionDst);

                    sourceAdapter.Fill(dataSetSrc);
                    destAdapter.Fill(dataSetDst);

                    var tableEnumSrc = dataSetSrc.Tables.GetEnumerator();
                    var tableEnumDst = dataSetDst.Tables.GetEnumerator();

                    // Enumerators start at NULL when created, so they need to advance to the first table
                    tableEnumSrc.MoveNext();
                    tableEnumDst.MoveNext();

                    DataTable tableSrc = (DataTable)tableEnumSrc.Current;
                    DataTable tableDst = (DataTable)tableEnumDst.Current;

                    var rowEnumSrc = tableSrc.Rows.GetEnumerator();

                    // Begin iterating through every row in the source query
                    while (rowEnumSrc.MoveNext())
                    {
                        DataRow rowSrc = (DataRow)rowEnumSrc.Current;

                        string insertString =   "INSERT INTO Sales.Test (OrderID, CustomerID, SalespersonPersonID, " +
                                                "PickedByPersonID, ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate, " +
                                                "CustomerPurchaseOrderNumber, IsUndersupplyBackordered, Comments, DeliveryInstructions, " +
                                                "InternalComments, PickingCompletedWhen, LastEditedBy, LastEditedWhen) " +
                                                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

                        OdbcCommand command = connectionSrc.CreateCommand();
                        command.Connection  = connectionSrc;
                        command.CommandText = insertString;

                        // Setup for duplicate detection
                        var rowEnumDst  = tableDst.Rows.GetEnumerator();
                        bool matchFound = false;

                        // Duplicate detection happens here and skips the current row if there is a match
                        while (!matchFound && rowEnumDst.MoveNext())
                        {
                            DataRow rowDst = (DataRow)rowEnumDst.Current;
                            matchFound = rowDst[0].Equals(rowSrc[0]);
                        }

                        if (matchFound) continue;

                        command.Parameters.Add("@OrderID",                      OdbcType.Int).Value         = rowSrc[0];
                        command.Parameters.Add("@CustomerID",                   OdbcType.Int).Value         = rowSrc[1];
                        command.Parameters.Add("@SalespersonPersonID",          OdbcType.Int).Value         = rowSrc[2];
                        command.Parameters.Add("@PickedByPersonID",             OdbcType.Int).Value         = rowSrc[3];
                        command.Parameters.Add("@ContactPersonID",              OdbcType.Int).Value         = rowSrc[4];
                        command.Parameters.Add("@BackorderOrderID",             OdbcType.Int).Value         = rowSrc[5];
                        command.Parameters.Add("@OrderDate",                    OdbcType.Date).Value        = rowSrc[6];
                        command.Parameters.Add("@ExpectedDeliveryDate",         OdbcType.Date).Value        = rowSrc[7];
                        command.Parameters.Add("@CustomerPurchaseOrderNumber",  OdbcType.NVarChar).Value    = rowSrc[8];
                        command.Parameters.Add("@IsUndersupplyBackordered",     OdbcType.Bit).Value         = rowSrc[9];
                        command.Parameters.Add("@Comments",                     OdbcType.NVarChar).Value    = rowSrc[10];
                        command.Parameters.Add("@DeliveryInstructions",         OdbcType.NVarChar).Value    = rowSrc[11];
                        command.Parameters.Add("@InternalComments",             OdbcType.NVarChar).Value    = rowSrc[12];
                        command.Parameters.Add("@PickingCompletedWhen",         OdbcType.DateTime).Value    = rowSrc[13];
                        command.Parameters.Add("@LasEditedBy",                  OdbcType.Int).Value         = rowSrc[14];
                        command.Parameters.Add("@LastEditedWhen",               OdbcType.DateTime).Value    = rowSrc[15];

                        destAdapter.InsertCommand = command;

                        destAdapter.InsertCommand.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
