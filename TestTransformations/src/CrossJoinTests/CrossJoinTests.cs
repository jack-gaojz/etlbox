using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using System.Collections.Generic;
using System.Reflection.Metadata;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CrossJoinTests
    {
        public CrossJoinTests()
        {
        }

        [Fact]
        public void CrossJoinStringWithInt()
        {
            //Arrange
            MemorySource<string> source1 = new MemorySource<string>();
            source1.DataAsList = new List<string>() { "A", "B" };
            MemorySource<int> source2 = new MemorySource<int>();
            source2.DataAsList = new List<int>() { 1, 2, 3 };
            CrossJoin<string, int, string> crossJoin = new CrossJoin<string, int, string>();
            crossJoin.CrossJoinFunc = (data1, data2) => data1 + data2.ToString();
            MemoryDestination<string> dest = new MemoryDestination<string>();


            //Act
            source1.LinkTo(crossJoin.InMemoryTarget);
            source2.LinkTo(crossJoin.PassingTarget);
            crossJoin.LinkTo(dest);
            source1.Execute();
            source2.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(6, dest.Data.Count);
            Assert.Collection<string>(dest.Data,
                s => Assert.Equal("A1", s),
                s => Assert.Equal("B1", s),
                s => Assert.Equal("A2", s),
                s => Assert.Equal("B2", s),
                s => Assert.Equal("A3", s),
                s => Assert.Equal("B3", s)
                );
        }

        [Fact]
        public void HigherLoadInPassingTarget()
        {
            //Arrange
            MemorySource<string> source1 = new MemorySource<string>();
            source1.Data = new List<string>() { "A", "B", "C", "D" };
            MemorySource<int> source2 = new MemorySource<int>();
            var source2List = new List<int>();
            for (int i = 0; i < 100000; i++) source2List.Add(i);
            source2.Data = source2List;
            CrossJoin<string, int, string> crossJoin = new CrossJoin<string, int, string>();
            crossJoin.CrossJoinFunc = (data1, data2) => data1 + data2.ToString();
            MemoryDestination<string> dest = new MemoryDestination<string>();


            //Act
            source1.LinkTo(crossJoin.InMemoryTarget);
            source2.LinkTo(crossJoin.PassingTarget);
            crossJoin.LinkTo(dest);
            source1.Execute();
            source2.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(400000, dest.Data.Count);
        }

        [Fact]
        public void HigherLoadInMemoryTarget()
        {
            //Arrange
            MemorySource<int> source1 = new MemorySource<int>();
            var source1List = new List<int>();
            for (int i = 0; i < 100000; i++) source1List.Add(i);
            source1.Data = source1List;
            MemorySource<int> source2 = new MemorySource<int>();
            var source2List = new List<int>() { 1, 2, 3 };
            source2.Data = source2List;
            CrossJoin<int,int, string> crossJoin = new CrossJoin<int, int, string>();
            crossJoin.CrossJoinFunc = (data1, data2) => data1 + data2.ToString();
            MemoryDestination<string> dest = new MemoryDestination<string>();


            //Act
            source1.LinkTo(crossJoin.InMemoryTarget);
            source2.LinkTo(crossJoin.PassingTarget);
            crossJoin.LinkTo(dest);
            source1.Execute();
            source2.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(300000, dest.Data.Count);
        }

    }
}
