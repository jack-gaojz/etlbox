using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class MergeJoinErrorLinkingTests
    {

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void ExceptionInJoinFunc()
        {
            //Arrange
            MemorySource<MySimpleRow> source1 = new MemorySource<MySimpleRow>();
            source1.DataAsList.Add(new MySimpleRow() { Col1 = 1, Col2 = "Marilyn" });
            MemorySource<MySimpleRow> source2 = new MemorySource<MySimpleRow>();
            source2.DataAsList.Add(new MySimpleRow() { Col1 = 1, Col2 = "Monroe" });

            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();
            MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow> join = new MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow>(
                (inputRow1, inputRow2) =>
                {
                    throw new NotSupportedException("Test");
                });

            source1.LinkTo(join.LeftJoinTarget);
            source2.LinkTo(join.RightJoinTarget);
            join.LinkTo(dest);

            //Act && Assert
            Assert.Throws<NotSupportedException>(() =>
            {
                try
                {
                    source1.Execute();
                    source2.Execute();
                    dest.Wait();
                }
                catch (AggregateException ae)
                {
                    throw ae.InnerException;
                }
            });

        }

        [Fact]
        public void ExceptionWhenEmptyingQueues()
        {
            //Arrange
            MemorySource<MySimpleRow> source1 = new MemorySource<MySimpleRow>();
            source1.DataAsList.Add(new MySimpleRow() { Col1 = 1, Col2 = "Marilyn" });
            source1.DataAsList.Add(new MySimpleRow() { Col1 = 2, Col2 = "Exception" });
            MemorySource<MySimpleRow> source2 = new MemorySource<MySimpleRow>();
            source2.DataAsList.Add(new MySimpleRow() { Col1 = 1, Col2 = "Monroe" });

            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();
            MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow> join = new MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow>(
                (inputRow1, inputRow2) =>
                {
                    if (inputRow2 != null)
                        return new MySimpleRow()
                        {
                            Col1 = inputRow1.Col1,
                            Col2 = inputRow1.Col2 + " " + (inputRow2.Col2 ?? "X")
                        };
                    else
                        throw new NotSupportedException("Test");
                });

            source1.LinkTo(join.LeftJoinTarget);
            source2.LinkTo(join.RightJoinTarget);
            join.LinkTo(dest);

            //Act & Assert
            Assert.Throws<NotSupportedException>(() =>
            {
                try
                {
                    source1.Execute();
                    source2.Execute();
                    dest.Wait();
                }
                catch (AggregateException ae)
                {
                    throw ae.InnerException;
                }
            });
        }

        [Fact]
        public void RedirectingErrorsInJoinFunc()
        {
            //Arrange
            MemorySource<MySimpleRow> source1 = new MemorySource<MySimpleRow>();
            source1.DataAsList.Add(new MySimpleRow() { Col1 = 1, Col2 = "Marilyn" });
            source1.DataAsList.Add(new MySimpleRow() { Col1 = 2, Col2 = "James" });
            source1.DataAsList.Add(new MySimpleRow() { Col1 = 3, Col2 = "Elvis" });
            MemorySource<MySimpleRow> source2 = new MemorySource<MySimpleRow>();
            source2.DataAsList.Add(new MySimpleRow() { Col1 = 1, Col2 = "Monroe" });
            source2.DataAsList.Add(new MySimpleRow() { Col1 = 2, Col2 = "Dean" });
            source2.DataAsList.Add(new MySimpleRow() { Col1 = 3, Col2 = "Presley" });

            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow> join = new MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow>(
                (inputRow1, inputRow2) =>
                {
                    if (inputRow1.Col1 == 2)
                        throw new Exception("test");
                    else return new MySimpleRow()
                    {
                        Col1 = inputRow1.Col1,
                        Col2 = inputRow1.Col2 + " " + (inputRow2.Col2 ?? "X")
                    };
                });

            source1.LinkTo(join.LeftJoinTarget);
            source2.LinkTo(join.RightJoinTarget);
            join.LinkTo(dest);
            join.LinkErrorTo(errorDest);

            //Act

            source1.Execute();
            source2.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            Assert.Collection(dest.Data,
                r => Assert.True(r.Col1 == 1 && r.Col2 == "Marilyn Monroe"),
                r => Assert.True(r.Col1 == 3 && r.Col2 == "Elvis Presley")
                );

            Assert.Collection<ETLBoxError>(errorDest.Data,
             d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText))
            );
        }

        [Fact]
        public void RedirectingErrorsWhenEmptyingQueue()
        {
            //Arrange
            MemorySource<MySimpleRow> source1 = new MemorySource<MySimpleRow>();
            source1.DataAsList.Add(new MySimpleRow() { Col1 = 1, Col2 = "Marilyn" });
            source1.DataAsList.Add(new MySimpleRow() { Col1 = 2, Col2 = "James" });
            MemorySource<MySimpleRow> source2 = new MemorySource<MySimpleRow>();
            source2.DataAsList.Add(new MySimpleRow() { Col1 = 1, Col2 = "Monroe" });

            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow> join = new MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow>(
                (inputRow1, inputRow2) =>
                {
                    if (inputRow2 == null)
                        throw new Exception("test");
                    else return new MySimpleRow()
                    {
                        Col1 = inputRow1.Col1,
                        Col2 = inputRow1.Col2 + " " + (inputRow2.Col2 ?? "X")
                    };
                });

            source1.LinkTo(join.LeftJoinTarget);
            source2.LinkTo(join.RightJoinTarget);
            join.LinkTo(dest);
            join.LinkErrorTo(errorDest);

            //Act

            source1.Execute();
            source2.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            Assert.Collection(dest.Data,
                r => Assert.True(r.Col1 == 1 && r.Col2 == "Marilyn Monroe")
                );

            Assert.Collection<ETLBoxError>(errorDest.Data,
             d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText))
            );
        }

    }
}
