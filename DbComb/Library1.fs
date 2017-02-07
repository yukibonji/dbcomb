namespace DbComb

open System
open System.Data
open System.Data.Sql
open System.Runtime.InteropServices

type ReaderContext(rdr:IDataReader) =
  let mutable _idx = 0

type DbRdr<'a> = ReaderContext -> 'a

[<Interface>]
type IReader<'a> =
  abstract Count : int
  abstract Consume : objs:obj[]* start:int * length:int * [<Out>] value:byref<'a> -> bool
  abstract member Drain : objs:obj[] * start:int * length:int * [<Out>] value:byref<'a> -> bool

type ReaderBuilding() =
  static member Combine(a:IReader<'a>, b:IReader<'b>) : IReader<'a*'b> =
    let count = a.Count + b.Count
    let mutable ra : 'a = Unchecked.defaultof<'a>
    let mutable rb : 'b = Unchecked.defaultof<'b>
    { new IReader<_> with
        member x.Consume(objs: obj [], start: int, length: int, value:byref<'a*'b>) : bool =
          let gota = a.Consume (objs,start,(start + a.Count - 1), &ra)
          let gotb = b.Consume (objs,(start + a.Count),length,&rb)
          match gota, gotb with
          | true, true ->
            value <- (ra, rb)
            true
          | false, false ->
            false
          | _ -> failwith "Reader error"
        member x.Drain(objs: obj [], start: int, length: int, value:byref<'a*'b>) : bool =
          true
        member x.Count: int = count }

  static member OneToMany(headCount:int, extractHeadPk:obj[]->'pk, childRdr:IReader<'child>, combine:obj[]->ResizeArray<'child>->'tfinal) : IReader<'tfinal> =
    let childRdrCount = childRdr.Count
    let count = headCount + childRdrCount
    let mutable headerPrev = Array.zeroCreate<obj>(headCount)
    let mutable headerCurrent = Array.zeroCreate<obj>(headCount)
    let mutable headerTmp : obj[] = Unchecked.defaultof<obj[]>
    let mutable oldPk : 'pk = Unchecked.defaultof<'pk>
    let mutable unclaimedData = false
    let children = ResizeArray<'child>()
    let inline rotate() =
      headerTmp <- headerPrev
      headerPrev <- headerCurrent
      headerCurrent <- headerTmp
    { new IReader<_> with
        member x.Count: int = count
        member x.Drain(objs: obj [], start: int, length: int, value: byref<'tfinal>): bool =
          if unclaimedData then
            value <- combine headerPrev children
            true
          else false
        member x.Consume(objs: obj[], start: int, length: int, value:byref<'tfinal>) : bool =
          Array.Copy(objs, start, headerCurrent, 0, headCount)
          let didOutput = 
            if unclaimedData then
              let pk = extractHeadPk headerCurrent
              if oldPk <> pk then
                value <- combine headerPrev children 
                oldPk <- pk
                true
              else false
            else false

          rotate()
          if didOutput then children.Clear()

          let childStart = start + headCount
          let mutable child = Unchecked.defaultof<'child>
          if childRdr.Consume(objs, childStart, childRdrCount, &child) then
            children.Add child

          unclaimedData <- true

          didOutput }
          
  static member Leaf(colCount:int, ctor: obj[] -> 'a) : IReader<'a> =
    let tmp = Array.zeroCreate<obj>(colCount)
    { new IReader<'a> with
        member x.Consume(objs: obj [], start: int, length: int, value: byref<'a>): bool =
          Array.Copy(objs, start, tmp, 0, length)
          value <- ctor tmp
          true
        member x.Count: int = colCount 
        member x.Drain(objs: obj [], start: int, length: int, value: byref<'a>): bool =
          failwith "Nothing to drain" }

module R =
  let readTop (rdr:IDataReader) (r:IReader<'a>) : ResizeArray<'a> =
    let fieldCount = rdr.FieldCount
    let results = ResizeArray<'a>()
    let os = Array.zeroCreate(fieldCount)
    let rcount = r.Count
    let mutable isCont = false
    let mutable res = Unchecked.defaultof<'a>
    while rdr.Read() do
      let _ = rdr.GetValues(os)
      if r.Consume(os,0,rcount,&res) then
        let tmp = res // compiler bug without this tmp variable?
        results.Add(tmp)
        
    if r.Drain(os,0,rcount,&res) then
      let tmp = res
      results.Add(res)

    results
      
module Ops =
  let andThen (ra:DbRdr<'a>) (rb:DbRdr<'b>) : DbRdr<'a*'b> =
    failwith ""

module DbComb =
  let read (ctx:ReaderContext) (rdr:DbRdr<'a>) :'a =
    rdr ctx