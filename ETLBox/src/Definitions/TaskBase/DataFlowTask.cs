using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowTask : GenericTask, ITask
    {
        public List<DataFlowTask> Predecessors { get; set; } = new List<DataFlowTask>();
        public List<DataFlowTask> Successors { get; set; } = new List<DataFlowTask>();

        public Task Completion { get; set; }

        public Task PredecessorCompletion { get; set; }
        //CancellationTokenSource tokenSource = new CancellationTokenSource();
        //CancellationToken? token => tokenSource?.Token ?? null;

        protected int? _loggingThresholdRows;
        public virtual int? LoggingThresholdRows
        {
            get
            {
                if ((DataFlow.LoggingThresholdRows ?? 0) > 0)
                    return DataFlow.LoggingThresholdRows;
                else
                    return _loggingThresholdRows;
            }
            set
            {
                _loggingThresholdRows = value;
            }
        }

        public int ProgressCount { get; set; }

        protected bool HasLoggingThresholdRows => LoggingThresholdRows != null && LoggingThresholdRows > 0;
        protected int ThresholdCount { get; set; } = 1;

        public int MaxBufferSize
        {
            get
            {
                return _maxBufferSize > 0 ? _maxBufferSize : DataFlow.MaxBufferSize;
            }
            set
            {
                _maxBufferSize = value;
            }
        }

        protected int _maxBufferSize = -1;

        protected virtual void InitBufferObjects() { }

        protected bool WereBufferInitialized;
        protected Dictionary<DataFlowTask, bool> WasLinked = new Dictionary<DataFlowTask, bool>();
        protected Dictionary<DataFlowTask, Tuple<object,object>> LinkPredicates= new Dictionary<DataFlowTask, Tuple<object,object>>();

        protected void InitBufferRecursively()
        {
            foreach (DataFlowTask predecessor in Predecessors)
            {
                if (!predecessor.WereBufferInitialized)
                    predecessor.InitBufferRecursively();
            }

            if (!WereBufferInitialized)
            {
                InitBufferObjects();
                WereBufferInitialized = true;
            }

            foreach (DataFlowTask successor in Successors)
            {
                if (!successor.WereBufferInitialized)
                    successor.InitBufferRecursively();
            }

        }

        public DataFlowTask LinkTo2(DataFlowTask target)
        {
            this.Successors.Add(target);
            target.Predecessors.Add(this);
            return target;// as IDataFlowLinkSource<TOutput>;
        }

        public DataFlowTask LinkTo2(DataFlowTask target, object predicate)
        {
            LinkPredicates.Add(target, Tuple.Create<object,object>(predicate, null));
            return LinkTo2(target);
        }

        public DataFlowTask LinkTo2(DataFlowTask target, object predicate, object voidPredicate)
        {
            LinkPredicates.Add(target, Tuple.Create<object,object>(predicate, voidPredicate));
            return LinkTo2(target);
        }

        protected void LinkBuffersRecursively()
        {
            foreach (DataFlowTask predecessor in Predecessors)
            {
                if (!predecessor.WasLinked.ContainsKey(this))
                {
                    Tuple<object,object> predicate = null;
                    LinkPredicates.TryGetValue(this, out predicate);
                    predecessor.LinkBuffers(this, predicate);
                    predecessor.WasLinked.Add(this, true);
                    predecessor.LinkBuffersRecursively();
                }
            }
            foreach (DataFlowTask successor in Successors)
            {
                if (!WasLinked.ContainsKey(successor))
                {
                    Tuple<object,object> predicate = null;
                    LinkPredicates.TryGetValue(successor, out predicate);
                    LinkBuffers(successor, predicate);
                    WasLinked.Add(successor, true);
                    successor.LinkBuffersRecursively();
                }
            }
        }
        protected virtual void LinkBuffers(DataFlowTask successor, Tuple<object,object> predicate)
        {
            //TODO throw new exception that a destionation can't link or something
        }


        protected void SetCompletionTaskRecursively()
        {

            foreach (DataFlowTask predecessor in Predecessors)
            {
                if (predecessor.Completion == null)
                    predecessor.SetCompletionTaskRecursively();
            }

            if (Completion == null)
            {
                List<Task> CompletionTasks = CollectCompletionFromPredecessors();
                if (CompletionTasks.Count > 0)
                {
                    PredecessorCompletion = Task.WhenAll(CompletionTasks).ContinueWith(CompleteOrFaultBuffer);
                    Completion = Task.WhenAll(PredecessorCompletion, BufferCompletion).ContinueWith(CompleteOrFaultCompletion);
                }
            }

            foreach (DataFlowTask successor in Successors)
            {
                if (successor.Completion == null)
                    successor.SetCompletionTaskRecursively();
            }
        }

        private List<Task> CollectCompletionFromPredecessors()
        {
            List<Task> CompletionTasks = new List<Task>();
            foreach (DataFlowTask pre in Predecessors)
            {
                CompletionTasks.Add(pre.Completion);
                CompletionTasks.Add(pre.BufferCompletion);
            }
            return CompletionTasks;
        }

        protected void FaultPredecessorsRecursively(Exception e)
        {
            FaultBuffer(e);
            foreach (DataFlowTask pre in Predecessors)
                pre.FaultPredecessorsRecursively(e);
        }


        protected virtual void FaultBuffer(Exception e)
        {

        }

        protected virtual void CompleteOrFaultBuffer(Task t)
        {
        }

        protected virtual void CompleteOrFaultCompletion(Task t)
        {
            if (t.IsFaulted)

            {
                CleanUpOnFaulted(t.Exception.Flatten());
                //throw t.Exception.Flatten();
            }
            else
                CleanUpOnSuccess();
        }

        protected virtual void CleanUpOnSuccess()
        {

        }

        protected virtual void CleanUpOnFaulted(Exception e)
        {

        }

        //protected virtual void CompleteOrFaultCompletion(Task t)
        //{
        //        if (t.IsFaulted)
        //            throw t.Exception.Flatten();
        //}

        protected virtual Task BufferCompletion { get; }

        protected void NLogStart()
        {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

        protected void NLogFinish()
        {
            if (!DisableLogging && HasLoggingThresholdRows)
                NLogger.Info(TaskName + $" processed {ProgressCount} records in total.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

        protected void LogProgressBatch(int rowsProcessed)
        {
            ProgressCount += rowsProcessed;
            if (!DisableLogging && HasLoggingThresholdRows && ProgressCount >= (LoggingThresholdRows * ThresholdCount))
            {
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
                ThresholdCount++;
            }
        }

        protected void LogProgress()
        {
            ProgressCount += 1;
            if (!DisableLogging && HasLoggingThresholdRows && (ProgressCount % LoggingThresholdRows == 0))
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

    }

}
