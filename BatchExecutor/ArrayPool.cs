/*
   Copyright 2017 Igor Brylin

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
using System;
using System.Collections.Concurrent;

namespace BatchExecutor
{
	internal class ArrayPool<T>
	{
		private readonly ConcurrentStack<T[]> _container = new ConcurrentStack<T[]>();

		public T[] GetOrCreate(int size)
		{
			if (!_container.TryPop(out var result))
				result = new T[size];

			return result;
		}

		public void Release(T[] array)
		{
            Array.Clear(array, 0, array.Length);
			_container.Push(array);
		}
	}
}