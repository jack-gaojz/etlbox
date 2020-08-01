using ETLBox.Connection;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class MemorySourceLinkingTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public MemorySourceLinkingTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void LinkingMemoryConnectors()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            RowTransformation<MySimpleRow> row = new RowTransformation<MySimpleRow>();
            row.TransformationFunc = row => row;
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act
            source.DataAsList = new List<MySimpleRow>()
            {
                new MySimpleRow() { Col1 = 1, Col2 = "Test1" },
                new MySimpleRow() { Col1 = 2, Col2 = "Test2" },
                new MySimpleRow() { Col1 = 3, Col2 = "Test3" }
            };
            source.LinkTo2(row);
            row.LinkTo2(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, dest.Data.Count);
        }

        [Fact]
        public void LinkingMemoryConnectorsWithError()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            RowTransformation<MySimpleRow> row = new RowTransformation<MySimpleRow>();
            row.TransformationFunc =
                row =>
                {
                    if (row.Col1 == 2) throw new Exception($"{row.Col2}");
                    return row;
                };
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();
            for (int i = 0; i<=10000000; i++)
            {
                source.DataAsList.Add(new MySimpleRow() { Col1 = i, Col2 = $"Test{i}" });
            }

            //Act
            source.LinkTo2(row);
            row.LinkTo2(dest);
            try
            {
                source.Execute();
                dest.Wait();

            }
            catch (Exception e)
            {
                Assert.True(source.SourceBlock.Completion.Status == System.Threading.Tasks.TaskStatus.Faulted);
            }

        }


        [Fact]
        public async void LinkingMemoryConnectorsWithErrorAsync()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            RowTransformation<MySimpleRow> row = new RowTransformation<MySimpleRow>();
            row.TransformationFunc =
                row =>
                {
                    if (row.Col1 == 2) throw new Exception($"{row.Col2}");
                    return row;
                };
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();
            for (int i = 0; i < 10000000; i++)
            {
                source.DataAsList.Add(new MySimpleRow() { Col1 = i, Col2 = $"Test{i}" });
            }

            //Act
            source.LinkTo2(row);
            row.LinkTo2(dest);
            try
            {
                await source.ExecuteAsync();
                await dest.Completion;


            }
            catch (Exception e)
            {

                Assert.True(source.SourceBlock.Completion.Status == System.Threading.Tasks.TaskStatus.Faulted);
            }

        }


        [Fact]
        public void LinkingMany()
        {
            /*
             * 
             *  Source1 ---v                        |--> Destination1
             *             |--> Row --> Multicast --|
             *  Source2 ---^                        |--> Destination 2
             */
            //Arrange
            MemorySource<MySimpleRow> source1 = new MemorySource<MySimpleRow>();
            MemorySource<MySimpleRow> source2 = new MemorySource<MySimpleRow>();
            RowTransformation<MySimpleRow> row = new RowTransformation<MySimpleRow>();
            row.TransformationFunc =
                row => row;
            Multicast<MySimpleRow> multi = new Multicast<MySimpleRow>();
            MemoryDestination<MySimpleRow> dest1 = new MemoryDestination<MySimpleRow>();
            MemoryDestination<MySimpleRow> dest2 = new MemoryDestination<MySimpleRow>();

            //Act
            source1.DataAsList = new List<MySimpleRow>()
            {
                new MySimpleRow() { Col1 = 1, Col2 = "Test1" },
                new MySimpleRow() { Col1 = 2, Col2 = "Test2" },
                new MySimpleRow() { Col1 = 3, Col2 = "Test3" }
            };
            source2.DataAsList = new List<MySimpleRow>()
            {
                new MySimpleRow() { Col1 = 4, Col2 = "Test4" },
                new MySimpleRow() { Col1 = 5, Col2 = "Test5" }
            };
            source1.LinkTo2(row);
            source2.LinkTo2(row);
            row.LinkTo2(multi);
            multi.LinkTo2(dest1);
            multi.LinkTo2(dest2);

            source1.Execute();
            source2.Execute();
            dest1.Wait();
            dest2.Wait();

            //Assert
            Assert.Equal(5, dest1.Data.Count);
            Assert.Equal(5, dest2.Data.Count);
        }
    }
}
