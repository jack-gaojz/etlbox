using ETLBox.DataFlow;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace ETLBoxTests
{
    public class TPLDataFlowTests
    {
        [Fact]
        public void BatchBlockNotWorkingWithBoundedCapacity()
        {
            //Arrange
            var total = 10;
            var processed = 0;

            //BroadcastBlock does not send if bounded capacity is set and buffer in target is full
            //Message will be "lost"
            BroadcastBlock<int> bb = new BroadcastBlock<int>(c => c);
            ActionBlock<int> ab = new ActionBlock<int>(
               (messageUnit) =>
               {
                   Thread.Sleep(10);
                   processed++;
               },
                new ExecutionDataflowBlockOptions() { BoundedCapacity = 2 }
           );

            bb.LinkTo(ab, new DataflowLinkOptions() { PropagateCompletion = true });

            //Act
            for (int i = 0; i < total; i++)
                bb.SendAsync(i).Wait();

            bb.Complete();
            ab.Completion.Wait();

            //Assert
            Assert.NotEqual(total, processed); //Should be Assert.Equal(total, processed) if BroadcastBlock would wait!
        }

        [Fact]
        public void JoinBlockWithUnevenInput()
        {
            //Arrange
            List<Tuple<int, int>> result = new List<Tuple<int, int>>();
            var producer1 = new BufferBlock<int>();
            var producer2 = new BufferBlock<int>();

            var joinBlock = new JoinBlock<int, int>();

            producer1.LinkTo(joinBlock.Target1);
            producer2.LinkTo(joinBlock.Target2);

            var actionBlock = new ActionBlock<Tuple<int, int>>(value =>
            {
                result.Add(value);
            });

            joinBlock.LinkTo(actionBlock);

            //Act
            producer1.Post(1);
            producer2.Post(2);
            producer1.Post(3);

            producer1.Complete();
            producer1.Completion.Wait();
            producer2.Complete();
            producer2.Completion.Wait();
            joinBlock.Complete();
            joinBlock.Completion.Wait();
            actionBlock.Complete();
            actionBlock.Completion.Wait(); ;

            //Assert
            //The Mergejoin disregards the ueven input from producer1...
            Assert.Collection(result,
                r => Assert.True(r.Item1 == 1 && r.Item2 == 2)
                );


        }

        [Fact]
        public void WaitingForNullBlockNeverCompletes()
        {
            var producer1 = new BufferBlock<int>();
            var trash = DataflowBlock.NullTarget<int>();
            producer1.LinkTo(trash);

            producer1.Post(1);
            producer1.Complete();
            producer1.Completion.Wait();
            trash.Complete();

            Task.Delay(100).Wait();
            Assert.True(trash.Completion.Status != TaskStatus.RanToCompletion);
        }

    }
}
