import test from 'node:test';
import assert from 'node:assert/strict';
import {utc,fresh,temp,altitude,spread,stationRows,lcl,cloudLayers,safeImage} from '../model.mjs';
const now=Date.parse('2026-09-20T16:00:00Z');
const sample={temp_c:20,dew_c:19.5,elev_m:900,temp_ob_time:'2026-09-20T15:50:00',provider:'MesoWest'};
const rows=changes=>stationRows({stations:{SE068:{...sample,...changes}}},now);
const vor=changes=>rows(changes).find(r=>r.id==='SE068');

test('primary naive timestamps are UTC; future and old readings are excluded',()=>{
  assert.equal(utc('2026-09-20T16:00:00'),now);
  assert.equal(fresh('2026-09-20T16:01:00Z',60,now),false);
  assert.equal(fresh('2026-09-20T14:59:00Z',60,now),false);
  assert.equal(fresh(undefined,60,now),false);
});
test('unit conversion treats temperature differences without a 32 degree offset',()=>{
  assert.equal(temp(20,'imperial'),68);
  assert.equal(spread(1,'imperial'),1.8);
  assert.ok(Math.abs(altitude(304.8,'imperial')-1000)<1e-9);
});
test('near saturation uses paired valid observations and one degree threshold',()=>{
  assert.equal(vor().near,true);
  assert.equal(vor({dew_c:19}).near,true);
  assert.equal(vor({dew_c:18.9}).near,false);
  assert.equal(vor({dew_c:null}).near,false);
  assert.equal(vor({dew_c:21}).near,false);
});
test('cached, stale and future readings cannot imply cloud or estimate LCL',()=>{
  for(const changes of [{provider:'MesoWest (last-good)'},{temp_ob_time:'2026-09-20T14:00:00Z'},
    {temp_ob_time:'2026-09-20T17:00:00Z'},{dew_c:null}]) {
    assert.equal(vor(changes).near,false);
    assert.equal(lcl(rows(changes)),null);
  }
});
test('estimated LCL is above sea level and preserves zero spread',()=>{
  assert.equal(lcl(rows()),962.5);
  assert.equal(lcl(rows({dew_c:20})),900);
});
test('station rows keep missing data missing, never coerce null or numeric strings to zero',()=>{
  assert.equal(vor({temp_c:null}).current,false);
  assert.equal(vor({temp_c:'20'}).temp_c,null);
  assert.equal(vor({dew_c:NaN}).dew_c,null);
});
test('observed bases require fresh airport reports and valid layers',()=>{
  const airport={observed_at:'2026-09-20T15:00:00Z',layers:[{cover:'OVC',base_msl_m:368,base_agl_ft:1200}]};
  assert.equal(cloudLayers(airport,now).length,1);
  assert.equal(cloudLayers({...airport,observed_at:'2026-09-20T14:00:00Z'},now).length,0);
  assert.equal(cloudLayers({...airport,observed_at:'2026-09-20T17:00:00Z'},now).length,0);
  assert.equal(cloudLayers({...airport,layers:[{cover:'VV',base_msl_m:100,base_agl_ft:300}]},now).length,0);
});
test('only the selected public camera and dated NOAA images can load',()=>{
  const camera='https://img.cdn.prod.alertwest.com/data/img/1986/2026/09/20/Gibraltar_2_1789916476_2697.jpg';
  const satellite='https://cdn.star.nesdis.noaa.gov/WFO/lox/GEOCOLOR/20262631440_GOES18-ABI-lox-GEOCOLOR-600x600.jpg';
  assert.equal(safeImage(camera,'camera'),camera);
  assert.equal(safeImage(satellite,'satellite'),satellite);
  for(const bad of [camera.replace('/1986/','/1987/'),camera+'.evil.com',camera.replace('https:','http:'),'javascript:alert(1)']) assert.equal(safeImage(bad,'camera'),null);
});
