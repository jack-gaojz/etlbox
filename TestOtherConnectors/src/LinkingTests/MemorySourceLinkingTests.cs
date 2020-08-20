using ETLBox.Connection;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;
using Xunit;
using Xunit.Sdk;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class LinkingTests
    {
        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Theory]
        [InlineData("sync")]
        [InlineData("async")]
        public void SimpleFlow(string processing)
        {
            /*
             *  Source --> Row --> Destination
             */

            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.DataAsList = CreateDemoData(1, 3);

            RowTransformation<MySimpleRow> row = new RowTransformation<MySimpleRow>();
            row.TransformationFunc = row => row;
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act
            source.LinkTo(row);
            row.LinkTo(dest);

            if (processing == "sync")
            {
                source.Execute();
                dest.Wait();
            }
            else if (processing == "async")
            {
                Task t1 = source.ExecuteAsync();
                Task t2 = dest.Completion;
                Task.WaitAll(t1, t2);
            }

            //Assert
            Assert.Collection(dest.Data,
                row => Assert.True(row.Col1 == 1 && row.Col2 == "Test1"),
                row => Assert.True(row.Col1 == 2 && row.Col2 == "Test2"),
                row => Assert.True(row.Col1 == 3 && row.Col2 == "Test3")
            );
        }

        private static List<MySimpleRow> CreateDemoData(int start, int end)
        {
            var result = new List<MySimpleRow>();
            for (int i = start; i <= end; i++)
                result.Add(new MySimpleRow() { Col1 = i, Col2 = $"Test{i}" });
            return result;
        }


        [Theory]
        [InlineData(5, "sync")]
        [InlineData(100000, "sync")]
        [InlineData(5, "async")]
        [InlineData(100000, "async")]
        public void ErrorWhenExecuting(int rowsToProcess, string processing)
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.DataAsList = CreateDemoData(1, rowsToProcess);
            RowTransformation<MySimpleRow> row = new RowTransformation<MySimpleRow>();
            row.TransformationFunc =
                row =>
                {
                    if (row.Col1 == 2) throw new Exception($"{row.Col2}");
                    return row;
                };
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act
            source.LinkTo(row);
            row.LinkTo(dest);
            try
            {
                if (processing == "sync")
                {
                    source.Execute();
                    dest.Wait();
                    Assert.True(false);
                }
                else if (processing == "async")
                {
                    Task t1 = source.ExecuteAsync();
                    Task t2 = dest.Completion;
                    Task.WaitAll(t1, t2);
                }
            }
            catch (Exception e)
            {
                if (rowsToProcess < 10)
                {
                    Assert.True(source.SourceBlock.Completion.Status == System.Threading.Tasks.TaskStatus.RanToCompletion);
                    Assert.True(source.Completion.Status == System.Threading.Tasks.TaskStatus.RanToCompletion);
                }
                else
                {
                    Assert.True(source.SourceBlock.Completion.Status == System.Threading.Tasks.TaskStatus.Faulted);
                    Assert.True(source.Completion.Status == System.Threading.Tasks.TaskStatus.Faulted);
                }
                Assert.True(dest.TargetBlock.Completion.Status == System.Threading.Tasks.TaskStatus.Faulted);
                Assert.True(dest.Completion.Status == System.Threading.Tasks.TaskStatus.Faulted);
                Assert.True(row.Completion.Status == System.Threading.Tasks.TaskStatus.Faulted);
                Assert.True(row.SourceBlock.Completion.Status == System.Threading.Tasks.TaskStatus.Faulted);
                if (rowsToProcess < 10)
                    Assert.True(source.ProgressCount == rowsToProcess);
                else
                    Assert.True(source.ProgressCount < rowsToProcess);
            }
        }


        [Fact]
        public void LinkingErrorDestination()
        {
            /*
             *  Source  -------> Row --> Destination
             *    |(errors)
             *    ---------------------> ErrorDestination
             */

            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.DataAsList = CreateDemoData(1, 5);
            RowTransformation<MySimpleRow> row = new RowTransformation<MySimpleRow>();
            row.TransformationFunc =
                row =>
                {
                    if (row.Col1 == 2 || row.Col1 == 4) throw new Exception($"{row.Col2}");
                    return row;
                };
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            source.LinkTo(row);
            row.LinkTo(dest);
            row.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            Assert.Collection(dest.Data,
                row => Assert.True(row.Col1 == 1 && row.Col2 == "Test1"),
                row => Assert.True(row.Col1 == 3 && row.Col2 == "Test3"),
                row => Assert.True(row.Col1 == 5 && row.Col2 == "Test5")
            );
            Assert.True(errorDest.Data.Count == 2);
        }

        [Fact]
        public void LinkingSimpleMultitree()
        {
            /*
             *
             *  Source1 ---v                        |--> Destination1
             *             |--> Row --> Multicast --|
             *  Source2 ---^                        |--> Destination 2
             */
            //Arrange
            MemorySource<MySimpleRow> source1 = new MemorySource<MySimpleRow>();
            source1.DataAsList = CreateDemoData(1, 3);
            MemorySource<MySimpleRow> source2 = new MemorySource<MySimpleRow>();
            source2.DataAsList = CreateDemoData(4, 5);
            RowTransformation<MySimpleRow> row = new RowTransformation<MySimpleRow>();
            row.TransformationFunc = row => row;
            Multicast<MySimpleRow> multi = new Multicast<MySimpleRow>();
            MemoryDestination<MySimpleRow> dest1 = new MemoryDestination<MySimpleRow>();
            MemoryDestination<MySimpleRow> dest2 = new MemoryDestination<MySimpleRow>();

            //Act
            source1.LinkTo(row);
            source2.LinkTo(row);
            row.LinkTo(multi);
            multi.LinkTo(dest1);
            multi.LinkTo(dest2);

            source1.Execute();
            source2.Execute();
            dest1.Wait();
            dest2.Wait();

            //Assert
            dest1.Data.Should().BeEquivalentTo(CreateDemoData(1, 5));
            dest2.Data.Should().BeEquivalentTo(CreateDemoData(1, 5));
        }

        [Fact]
        public void LinkingUnevenMultitree()
        {
            /*
             *
             *  Source1 ---|                         |--> Destination1
             *             |--> Row1 --> Multicast --|
             *  Source2 ---|          |              |--> Destination 2
             *  Source3 ------> Row2  |
             */
            //Arrange
            MemorySource<MySimpleRow> source1 = new MemorySource<MySimpleRow>();
            source1.DataAsList = CreateDemoData(1, 3);
            MemorySource<MySimpleRow> source2 = new MemorySource<MySimpleRow>();
            source2.DataAsList = CreateDemoData(4, 5);
            MemorySource<MySimpleRow> source3 = new MemorySource<MySimpleRow>();
            source3.DataAsList = CreateDemoData(6, 6);
            RowTransformation<MySimpleRow> row1 = new RowTransformation<MySimpleRow>();
            row1.TransformationFunc = row => row;
            RowTransformation<MySimpleRow> row2 = new RowTransformation<MySimpleRow>();
            row2.TransformationFunc = row => row;
            Multicast<MySimpleRow> multi = new Multicast<MySimpleRow>();
            MemoryDestination<MySimpleRow> dest1 = new MemoryDestination<MySimpleRow>();
            MemoryDestination<MySimpleRow> dest2 = new MemoryDestination<MySimpleRow>();

            //Act

            source1.LinkTo(row1);
            source2.LinkTo(row1);
            row1.LinkTo(multi);
            multi.LinkTo(dest1);
            multi.LinkTo(dest2);
            source3.LinkTo(row2);
            row2.LinkTo(multi);
            source1.Execute();
            source2.Execute();
            source3.Execute();
            dest1.Wait();
            dest2.Wait();

            //Assert
            dest1.Data.Should().BeEquivalentTo(CreateDemoData(1, 6));
            dest2.Data.Should().BeEquivalentTo(CreateDemoData(1, 6));
        }


        [Fact]
        public void LinkingWithPredicate()
        {
            /*                   |- p1: _ = 1 --> Dest1
             *  Source --> Row --|
             *                   |- p2: _ > 1 --> Dest2
             */

            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.DataAsList = CreateDemoData(1, 3);
            RowTransformation<MySimpleRow> row = new RowTransformation<MySimpleRow>();
            row.TransformationFunc = row => row;
            MemoryDestination<MySimpleRow> dest1 = new MemoryDestination<MySimpleRow>();
            MemoryDestination<MySimpleRow> dest2 = new MemoryDestination<MySimpleRow>();

            //Act
            Predicate<MySimpleRow> p1 = new Predicate<MySimpleRow>(row => row.Col1 == 1);
            Predicate<MySimpleRow> p2 = new Predicate<MySimpleRow>(row => row.Col1 > 1);
            source.LinkTo(row);
            row.LinkTo(dest1, p1);
            row.LinkTo(dest2, p2);
            source.Execute();
            dest1.Wait();
            dest2.Wait();

            //Assert
            Assert.Collection(dest1.Data,
               row => Assert.True(row.Col1 == 1 && row.Col2 == "Test1")
            );
            Assert.Collection(dest2.Data,
              row => Assert.True(row.Col1 == 2 && row.Col2 == "Test2"),
              row => Assert.True(row.Col1 == 3 && row.Col2 == "Test3")
            );
        }

        [Fact]
        public void LinkingWithVoidPredicate()
        {
            /*
             *  Source --> Row --|- p1: _ = 1 --> Dest1
             *                   |- p2: _ > 1 --> (Void)
             */

            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.DataAsList = CreateDemoData(1, 3);
            RowTransformation<MySimpleRow> row = new RowTransformation<MySimpleRow>();
            row.TransformationFunc = row => row;
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act
            Predicate<MySimpleRow> p1 = new Predicate<MySimpleRow>(row => row.Col1 == 1);
            Predicate<MySimpleRow> p2 = new Predicate<MySimpleRow>(row => row.Col1 > 1);
            source.LinkTo(row);
            row.LinkTo(dest, p1, p2);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(dest.Data,
             row => Assert.True(row.Col1 == 1 && row.Col2 == "Test1")
            );
        }

        [Fact]
        public void TypeChange()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.DataAsList = CreateDemoData(1, 3);

            RowTransformation<MySimpleRow, ExpandoObject> row = new RowTransformation<MySimpleRow, ExpandoObject>();
            row.TransformationFunc = row =>
            {
                dynamic r = new ExpandoObject();
                r.Col1 = row.Col1;
                r.Col2 = row.Col2;
                return r;
            };
            MemoryDestination dest = new MemoryDestination();

            //Act
            source.LinkTo(row);
            row.LinkTo(dest);

            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(dest.Data,
                row => { dynamic r = row as ExpandoObject; Assert.True(r.Col1 == 1 && r.Col2 == "Test1"); },
                row => { dynamic r = row as ExpandoObject; Assert.True(r.Col1 == 2 && r.Col2 == "Test2"); },
                row => { dynamic r = row as ExpandoObject; Assert.True(r.Col1 == 3 && r.Col2 == "Test3"); }
            );
        }

        [Fact]
        public void Chaining()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.DataAsList = CreateDemoData(1, 3);

            RowTransformation<MySimpleRow, ExpandoObject> row = new RowTransformation<MySimpleRow, ExpandoObject>();
            row.TransformationFunc = row =>
            {
                dynamic r = new ExpandoObject();
                r.Col1 = row.Col1;
                r.Col2 = row.Col2;
                return r;
            };
            RowTransformation<ExpandoObject, MySimpleRow> row2 = new RowTransformation<ExpandoObject, MySimpleRow>();
            row2.TransformationFunc = row =>
                {
                    dynamic r = row as ExpandoObject;
                    return new MySimpleRow { Col1 = r.Col1, Col2 = r.Col2 };
                };
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act
            source.LinkTo<ExpandoObject>(row).LinkTo<MySimpleRow>(row2).LinkTo(dest);

            source.Execute();
            dest.Wait();

            //Assert
            dest.Data.Should().BeEquivalentTo(CreateDemoData(1, 3));
        }

        [Fact]
        public void ErrorInOnCompletion()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.DataAsList = CreateDemoData(1, 3);

            RowTransformation<MySimpleRow> row = new RowTransformation<MySimpleRow>();
            row.TransformationFunc = row => row;
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            dest.OnCompletion = () =>
            {
                throw new Exception("Test1");
            };

            //Act

            source.LinkTo(row).LinkTo(dest);

            Assert.Throws<AggregateException>(() =>
           {
               source.Execute();
               dest.Wait();
           });


            //Assert
            dest.Data.Should().BeEquivalentTo(CreateDemoData(1, 3));
        }

    }
}
