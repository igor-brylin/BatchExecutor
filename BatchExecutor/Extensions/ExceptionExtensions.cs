using System;

namespace BatchExecutor.Extensions
{
	public static class ExceptionExtensions
	{
		public static Exception UnwrapAggregation(this Exception ex)
		{
			var aggrException = ex as AggregateException;
			if (aggrException != null)
				ex = aggrException.Flatten().InnerException;
			return ex;
		}
	}
}