using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using NLog.Targets;
using System;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowSource<TOutput> : DataFlowComponent, IDataFlowSource<TOutput>
    {
        /// <summary>
        /// SourceBlock from the underlying TPL.Dataflow which is used as output buffer for the component.
        /// </summary>
        public abstract ISourceBlock<TOutput> SourceBlock { get; }

        internal override void LinkBuffers(DataFlowComponent successor, LinkPredicates linkPredicates)
        {
            var s = successor as IDataFlowDestination<TOutput>;
            var linker = new BufferLinker<TOutput>(linkPredicates);
            linker.LinkBlocksWithPredicates(SourceBlock, s.TargetBlock);
        }

        /// <summary>
        /// Links the current block to another transformation or destination.
        /// Every component should be linked to only one component without predicates
        /// If you want to link multiple targets, either use predicates or a <see cref="ETLBox.DataFlow.Transformations.Multicast"/>
        /// </summary>
        /// <param name="target">Transformation or destination that the block is linked to.</param>
        /// <returns>The linked component.</returns>
        public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target)
            => InternalLinkTo<TOutput>(target);

        /// <summary>
        /// Links the current block to another transformation or destination with a predicate.
        /// Every component can be linked to one or more component. If you link multiple components,
        /// provide a <see cref="System.Predicate{TOutput}"/> that describe which row is send to which target.
        /// Make sure that all rows will be send to a target - use the <see cref="ETLBox.DataFlow.Connectors.VoidDestination"/>
        /// if you want to discared rows.
        /// </summary>
        /// <param name="target">Transformation or destination that the block is linked to.</param>
        /// <returns>The linked component.</returns>
        public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep)
           => InternalLinkTo<TOutput>(target, rowsToKeep);

        /// <summary>
        /// Links the current block to another transformation or destination with a predicate for rows that you want to keep
        /// and a second predicate for rows you want to discard.
        /// </summary>
        /// <param name="target">Transformation or destination that the block is linked to.</param>
        /// <returns>The linked component.</returns>
        public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => InternalLinkTo<TOutput>(target, rowsToKeep, rowsIntoVoid);

        /// <summary>
        /// Links the current block to another transformation or destination.
        /// Every component should be linked to only one component without predicates
        /// If you want to link multiple targets, either use predicates or a <see cref="ETLBox.DataFlow.Transformations.Multicast"/>
        /// </summary>
        /// <typeparam name="TConvert">Will convert the output type of the linked component.</typeparam>
        /// <param name="target">Transformation or destination that the block is linked to.</param>
        /// <returns>The linked component.</returns>
        public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target)
             => InternalLinkTo<TConvert>(target);

        /// <summary>
        /// Links the current block to another transformation or destination with a predicate.
        /// Every component can be linked to one or more component. If you link multiple components,
        /// provide a <see cref="System.Predicate{TOutput}"/> that describe which row is send to which target.
        /// Make sure that all rows will be send to a target - use the <see cref="ETLBox.DataFlow.Connectors.VoidDestination"/>
        /// if you want to discared rows.
        /// </summary>
        /// <typeparam name="TConvert">Will convert the output type of the linked component.</typeparam>
        /// <param name="target">Transformation or destination that the block is linked to.</param>
        /// <returns>The linked component.</returns>
        public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep)
            => InternalLinkTo<TConvert>(target, rowsToKeep);

        /// <summary>
        /// Links the current block to another transformation or destination with a predicate for rows that you want to keep
        /// and a second predicate for rows you want to discard.
        /// </summary>
        /// <typeparam name="TConvert">Will convert the output type of the linked component.</typeparam>
        /// <param name="target">Transformation or destination that the block is linked to.</param>
        /// <returns>The linked component.</returns>
        public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => InternalLinkTo<TConvert>(target, rowsToKeep, rowsIntoVoid);

        /// <summary>
        /// If an error occurs in the component, by default the component will throw an exception and stop execution.
        /// If you use the error linking, any erroneous records will catched and redirected.
        /// </summary>
        /// <param name="target">The target for erroneous rows.</param>
        /// <returns>The linked component.</returns>
        public IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
            => InternalLinkErrorTo(target);
    }
}
