using ETLBox.ControlFlow;
using NLog.Targets;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public class ActionJoinTarget<TInput> : DataFlowJoinTarget<TInput>
    {
        public override ITargetBlock<TInput> TargetBlock => JoinAction;
        ActionBlock<TInput> JoinAction;
        Action<TInput> Action;
        public ActionJoinTarget(DataFlowTask parent, Action<TInput> action)
        {
            Action = action;
            CreateLinkInInternalFlow(parent);
        }

        public override void InitBufferObjects()
        {
            JoinAction = new ActionBlock<TInput>(Action, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
        }

        protected override void CleanUpOnSuccess() { }

        protected override void CleanUpOnFaulted(Exception e) { }
    }
}
