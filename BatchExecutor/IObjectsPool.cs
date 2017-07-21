using System;

namespace BatchExecutor
{
	internal interface IObjectsPool<T>
	{
		T GetOrCreate(Func<T> factory);
		void Release(T obj);
	}
}