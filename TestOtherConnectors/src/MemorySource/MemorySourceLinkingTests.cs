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
            source.LinkTo(row);
            row.LinkTo(dest);
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
            for (int i=0;i<100000;i++)
            {
                source.DataAsList.Add(new MySimpleRow() { Col1 = i, Col2 = $"Test{i}" });
            }

            //Act
            source.LinkTo(row);
            row.LinkTo(dest);
            try
            {
                //dest.Wait();
               var x =  source.ExecuteAsync();
                //x.Wait();
                dest.Wait();

            }
            catch (Exception e)
            {
                //source.SourceBlock.Complete();
                //source.SourceBlock.Completion.Wait();
                source.SourceBlock.Fault(new Exception("x"));
                while (!source.SourceBlock.Completion.IsCompleted) { }
                //source.SourceBlock.Completion.GetAwaiter().GetResult();
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
            for (int i = 0; i < 100000; i++)
            {
                source.DataAsList.Add(new MySimpleRow() { Col1 = i, Col2 = $"Test{i}" });
            }

            //Act
            source.LinkTo(row);
            row.LinkTo(dest);
            try
            {
                //dest.Wait();
                var t1 = source.ExecuteAsync();
                //Task.WaitAll(t1, t2);
                await dest.TargetBlock.Completion;


            }
            catch (Exception e)
            {
                //source.SourceBlock.Complete();
                //source.SourceBlock.Completion.Wait();
                source.SourceBlock.Fault(new Exception("x"));
                while (!source.SourceBlock.Completion.IsCompleted) { }
                //source.SourceBlock.Completion.GetAwaiter().GetResult();
                Assert.True(source.SourceBlock.Completion.Status == System.Threading.Tasks.TaskStatus.Faulted);
            }

        }
    }
}
