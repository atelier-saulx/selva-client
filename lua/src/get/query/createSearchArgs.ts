// import { GetOptions } from '~selva/get/types'

// function createSearchArgs(getOptions: GetOptions, qeury: string): string[] {
//   const $list = getOptions.$list
//   if (!$list) {
//     return []
//   }
//   let lo = 0
//   let hi = 99999
//   if ($list.$range) {
//     lo = $list.$range[0]
//     hi = $list.$range[1]
//   }
//   const sort: string[] = []
//   if ($list.$sort) {
//     const $sort: string[] = (<any>$list).$sort
//     sort[sort.length] = 'SORTBY'
//     if (!Array.isArray($sort)) {
//       $list.$sort = [$sort]
//     }
//     for (let i = 0; i < $sort.length; i++) {
//       let { $field, $order } = $list.$sort[i]
//       if (!$order) {
//         $order = 'asc'
//       }
//       sort[sort.length] = $field
//       sort[sort.length] =
//         $order === 'asc' ? 'ASC' : $order === 'desc' ? 'DESC' : $order
//     }
//   }
//   const searchArgs: string[] = [
//     qeury,
//     'NOCONTENT',
//     'LIMIT',
//     tostring(lo),
//     tostring(hi),
//     ...sort
//   ]
//   return searchArgs
// }

// export default createSearchArgs
