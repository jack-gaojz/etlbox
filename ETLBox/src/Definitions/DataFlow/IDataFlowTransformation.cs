namespace ETLBox.DataFlow
{
    public interface IDataFlowTransformation<TInput, TOutput> : IDataFlowSource<TOutput>, IDataFlowDestination<TInput>
    { 

    }
}
