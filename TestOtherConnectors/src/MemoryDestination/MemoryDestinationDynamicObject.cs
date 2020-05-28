using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using System.Dynamic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class MemoryDestinationDynamicObjectTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public MemoryDestinationDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void DataIsInList()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("MemoryDestinationSource");
            source2Columns.InsertTestData();

            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>(SqlConnection, "MemoryDestinationSource");
            MemoryDestination<ExpandoObject> dest = new MemoryDestination<ExpandoObject>();

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            int index = 1;
            foreach (dynamic d in dest.Data)
            {

                Assert.True(d.Col1 == index && d.Col2 == "Test" + index);
                index++;
            }
        }


    }
}
