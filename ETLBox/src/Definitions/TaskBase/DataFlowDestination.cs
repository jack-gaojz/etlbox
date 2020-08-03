using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowDestination<TInput> : DataFlowTask, ITask, IDataFlowDestination<TInput>
    {
        public Action OnCompletion { get; set; }
        //public override Task Completion { get; set; }
        //protected Task Completion2 { get; set; }
        public ITargetBlock<TInput> TargetBlock => TargetAction;

        public virtual void Wait()
        {
            //while (Completion2 == null) { }
            //Completion2.Wait();
            //CheckCompleteAction();
            //Completion2.Wait();
            //Completion.Wait();
            //if (Completion.Status == TaskStatus.Faulted)
            //    throw Completion.Exception.Flatten();

            //try
            //{
                Completion.Wait();
            //}
            //catch (Exception e)
            //{
            //    ;
            //}
            //AwaitCompletion().Wait();
            //Completion.Wait();
        }

        protected ActionBlock<TInput> TargetAction { get; set; }
        //protected List<Task> PredecessorCompletions { get; set; } = new List<Task>();
        public ErrorHandler ErrorHandler { get; set; } = new ErrorHandler();

        //public void AddPredecessorCompletion(Task completion)
        //{
        //    PredecessorCompletions.Add(completion);
        //    //completion.ContinueWith(t =>
        //    //{
        //    //    CheckCompleteAction();
        //    //    //if (t.IsFaulted) throw t.Exception.Flatten();
        //    //});

        //}

        protected override Task BufferCompletion => TargetBlock.Completion;

        protected override void CompleteOrFaultOnPredecssorCompletion(Task t)
        {
            if (!TargetBlock.Completion.IsCompleted)
            {
                if (t.IsFaulted)
                {
                    TargetBlock.Fault(t.Exception.Flatten());
                    throw t.Exception.Flatten();
                }
                else TargetBlock.Complete();
            }
        }

        protected override void FaultBufferExplicitly(Exception e)
        {
            TargetBlock.Fault(e);
        }

        protected override void CleanUpOnFaulted(Exception e)
        {
            throw e;
        }

        public void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target)
             => ErrorHandler.LinkErrorTo(target, TargetAction.Completion);

        //protected void CheckCompleteAction()
        //{
        //    Completion2 = Task.WhenAll(PredecessorCompletions).ContinueWith(t =>
        //        {
        //            if (!TargetBlock.Completion.IsCompleted)
        //            {
        //                if (t.IsFaulted)
        //                {
        //                    TargetBlock.Fault(t.Exception.InnerException);
        //                }
        //                else TargetBlock.Complete();
        //            }
        //        });

        //}

        //protected void SetCompletionTask() => Completion = AwaitCompletion();

        protected virtual async Task AwaitCompletion()
        {
            try
            {
                await Completion.ConfigureAwait(false);
            }
            catch (AggregateException ae)
            {
                throw ae.Flatten();
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                CleanUp();
            }
        }

        protected virtual void CleanUp()
        {
            OnCompletion?.Invoke();
            NLogFinish();
        }
    }
}
