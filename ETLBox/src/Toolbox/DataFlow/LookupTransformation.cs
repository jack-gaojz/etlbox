using ETLBox.ControlFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.Exceptions;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// A lookup task - data from the input can be enriched with data retrieved from the lookup source.
    /// </summary>
    /// <typeparam name="TInput">Type of data input and output</typeparam>
    /// <typeparam name="TSourceOutput">Type of lookup data</typeparam>
    public class LookupTransformation<TInput, TSourceOutput> : DataFlowTransformation<TInput, TInput>
    {
        #region Public properties

        public override string TaskName { get; set; } = "Lookup";
        public List<TSourceOutput> LookupData
        {
            get
            {
                return LookupBuffer.Data as List<TSourceOutput>;
            }
            set
            {
                LookupBuffer.Data = value;
            }
        }// = new List<TSourceOutput>();
        public override ISourceBlock<TInput> SourceBlock => RowTransformation.SourceBlock;
        public override ITargetBlock<TInput> TargetBlock => RowTransformation.TargetBlock;
        public IDataFlowExecutableSource<TSourceOutput> Source { get; set; }

        public Func<TInput, TInput> TransformationFunc { get; set; }

        #endregion

        #region Constructors

        public LookupTransformation()
        {
            LookupBuffer = new MemoryDestination<TSourceOutput>();
            RowTransformation = new RowTransformation<TInput, TInput>();
        }

        public LookupTransformation(IDataFlowExecutableSource<TSourceOutput> lookupSource) : this()
        {
            Source = lookupSource;
        }

        public LookupTransformation(IDataFlowExecutableSource<TSourceOutput> lookupSource, Func<TInput, TInput> transformationFunc)
            : this(lookupSource)
        {
            TransformationFunc = transformationFunc;
        }

        public LookupTransformation(IDataFlowExecutableSource<TSourceOutput> lookupSource, Func<TInput, TInput> transformationFunc, List<TSourceOutput> lookupList)
            : this(lookupSource, transformationFunc)
        {
            LookupData = lookupList;
        }

        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {
            if (TransformationFunc == null)
                DefaultFuncWithMatchRetrieveAttributes();
            InitRowTransformationManually(LoadLookupData);

            LinkInternalLoadBufferFlow();
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        internal override void CompleteBufferOnPredecessorCompletion() => RowTransformation.CompleteBufferOnPredecessorCompletion();

        internal override void FaultBufferOnPredecessorCompletion(Exception e) => RowTransformation.FaultBufferOnPredecessorCompletion(e);

        public new IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
        {
            var errorSource = InternalLinkErrorTo(target);
            RowTransformation.ErrorSource = new ErrorSource() { Redirection = this.ErrorSource };
            Source.ErrorSource = new ErrorSource() { Redirection = this.ErrorSource };
            return errorSource;
        }

        #endregion

        #region Implementation

        MemoryDestination<TSourceOutput> LookupBuffer ;
        RowTransformation<TInput, TInput> RowTransformation;
        LookupTypeInfo TypeInfo;

        private void InitRowTransformationManually(Action initAction)
        {
            RowTransformation.TransformationFunc = TransformationFunc;
            RowTransformation.CopyTaskProperties(this);
            RowTransformation.InitAction = initAction;
            RowTransformation.MaxBufferSize = this.MaxBufferSize;
            RowTransformation.InitBufferObjects();
        }

        private void LinkInternalLoadBufferFlow()
        {
            if (Source == null) throw new ETLBoxException("You need to define a lookup source before using a LookupTransformation in a data flow");
            LookupBuffer.CopyTaskProperties(this);
            Source.LinkTo(LookupBuffer);
        }

        private void DefaultFuncWithMatchRetrieveAttributes()
        {
            TypeInfo = new LookupTypeInfo(typeof(TInput), typeof(TSourceOutput));
            if (TypeInfo.MatchColumns.Count == 0 || TypeInfo.RetrieveColumns.Count == 0)
                throw new ETLBoxException("Please define either a transformation function or use the MatchColumn / RetrieveColumn attributes.");
            TransformationFunc = FindRowByAttributes;
        }

        private TInput FindRowByAttributes(TInput row)
        {
            var lookupHit = LookupData.Find(e =>
            {
                bool same = true;
                foreach (var mc in TypeInfo.MatchColumns)
                {
                    same &= mc.PropInInput.GetValue(row).Equals(mc.PropInOutput.GetValue(e));
                    if (!same) break;
                }
                return same;
            });
            if (lookupHit != null)
            {
                foreach (var rc in TypeInfo.RetrieveColumns)
                {
                    var retrieveValue = rc.PropInOutput.GetValue(lookupHit);
                    rc.PropInInput.TrySetValue(row, retrieveValue);
                }
            }
            return row;
        }

        private void LoadLookupData()
        {
            NLogStartOnce();
            Source.Execute();
            LookupBuffer.Wait();
        }

        #endregion

    }

    /// <summary>
    /// A lookup task - data from the input can be enriched with data retrieved from the lookup source.
    /// The non generic implementation uses a dynamic object as input and lookup source.
    /// </summary>
    public class LookupTransformation : LookupTransformation<ExpandoObject, ExpandoObject>
    {
        public LookupTransformation() : base()
        { }

        public LookupTransformation(IDataFlowExecutableSource<ExpandoObject> lookupSource)
            : base(lookupSource)
        { }

        public LookupTransformation(IDataFlowExecutableSource<ExpandoObject> lookupSource, Func<ExpandoObject, ExpandoObject> transformationFunc)
            : base(lookupSource, transformationFunc)
        { }

        public LookupTransformation(IDataFlowExecutableSource<ExpandoObject> lookupSource, Func<ExpandoObject, ExpandoObject> transformationFunc, List<ExpandoObject> lookupList)
            : base(lookupSource, transformationFunc, lookupList)
        { }
    }

}
