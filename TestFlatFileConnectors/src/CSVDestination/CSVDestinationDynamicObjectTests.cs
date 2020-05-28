using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using System.Dynamic;
using System.IO;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CsvDestinationDynamicObjectTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CsvDestinationDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SimpleFlow()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("CSVDestDynamicObject");
            s2C.InsertTestDataSet3();
            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>(SqlConnection, "CSVDestDynamicObject");

            //Act
            CsvDestination<ExpandoObject> dest = new CsvDestination<ExpandoObject>("./SimpleWithDynamicObject.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("./SimpleWithDynamicObject.csv"),
                File.ReadAllText("res/CsvDestination/TwoColumnsSet3DynamicObject.csv"));
        }

    }
}
