(function () {
  'use strict';
  var svg = document.getElementById('lesson-chart');
  if (!svg) return;
  var controls = document.querySelectorAll('[data-stage]');
  var summary = document.getElementById('layer-summary');
  var stage = 1;
  var NS = 'http://www.w3.org/2000/svg';
  // A deliberately simplified, fixed example, never current weather.
  var stations = [
    {name: 'Airport', z: 10, t: 82},
    {name: 'AntFarm', z: 2355, t: 72},
    {name: 'La Cumbre', z: 3940, t: 74}
  ];
  var profile = [[1000,75],[1500,73],[2000,71],[2500,69],[3000,68],
    [3500,70],[4000,72],[4500,70],[5000,67],[5500,64],[6000,61],[6500,58]];
  var descriptions = [
    'Temperature increases to the right and altitude increases upward. A dashed dry-adiabatic line starts at 75 degrees Fahrenheit and 1,000 feet, cooling 5.4 degrees per 1,000 feet of climb.',
    'Orange squares add three surface stations, joined in elevation order. La Cumbre is 2 degrees warmer than AntFarm, 1,585 feet below: plus 1.3 degrees Fahrenheit per thousand feet. These are different places, not one air column.',
    'The black RASS profile adds measurements of the air over SBA. Between 3,000 and 4,000 feet it bends right, warming from 68 to 72 degrees: an inversion that may cap thermals. The last blue dot is the end of measurements, not the top of lift.'
  ];
  var summaries = ['A dry parcel cools as it climbs.', 'Surface stations compare different places.', 'RASS reveals the structure of the air aloft.'];

  function el(tag, attrs, content, parent) {
    var n = document.createElementNS(NS, tag);
    Object.keys(attrs || {}).forEach(function (key) { n.setAttribute(key, attrs[key]); });
    if (content !== undefined) n.textContent = content;
    (parent || svg).appendChild(n);
    return n;
  }
  function line(x1,y1,x2,y2,cls) { return el('line',{x1:x1,y1:y1,x2:x2,y2:y2,'class':cls || 'leader'}); }
  function text(x,y,words,cls,anchor) {
    return el('text',{x:x,y:y,'class':cls || 'label','text-anchor':anchor || 'start'},words);
  }
  function note(x,y,lines,cls) {
    lines.forEach(function (s,i) { text(x,y+i*19,s,'label '+(i ? 'quiet' : (cls || 'emphasis'))); });
  }
  function leader(points) { el('polyline',{points:points.map(function(p){return p.join(',');}).join(' '),'class':'leader'}); }
  function draw() {
    var width = Math.round(svg.parentElement.clientWidth);
    if (width < 1) return;
    var compact = width < 680;
    var left = compact ? 44 : 62;
    var right = compact ? width-12 : width-275;
    var top = compact ? 68 : 42;
    var bottom = compact ? 404 : 456;
    var height = compact ? 580 : 532;
    function x(t) { return left + (t-44)/42*(right-left); }
    function y(z) { return bottom - z/7000*(bottom-top); }
    function dry(z) { return 75-5.4*(z-1000)/1000; }
    svg.replaceChildren();
    svg.setAttribute('viewBox','0 0 '+width+' '+height);
    svg.setAttribute('height',height);
    svg.setAttribute('data-current-stage',stage);
    el('title',{id:'diagram-title'},'Step '+stage+': '+summaries[stage-1]);
    el('desc',{id:'diagram-description'},descriptions[stage-1]);

    // Identical physical scales and plot extents in all three layers.
    [0,2000,4000,6000].forEach(function(z) {
      line(left,y(z),right,y(z),'grid');
      if (!(stage === 2 && z === 4000)) {
        text(left-10,y(z)+4,z.toLocaleString('en-US'),'tick','end');
      }
    });
    [50,60,70,80].forEach(function(t) {
      line(x(t),top,x(t),bottom,'grid');
      text(x(t),bottom+22,String(t),'tick','middle');
    });
    line(left,top,left,bottom,'axis'); line(left,bottom,right,bottom,'axis');
    text(left,top-20,'Altitude (ft) ↑','axis-label');
    text(left,bottom+45,'Temperature (°F) → warmer','axis-label');
    el('path',{d:'M'+x(dry(0))+','+y(0)+' L'+x(dry(6500))+','+y(6500),'class':'dalr'});

    if (stage >= 2) {
      if (stage === 2) {
        line(left,y(3940),x(74),y(3940),'projection');
        line(x(74),y(3940),x(74),bottom,'projection');
        text(left-10,y(3940)+4,'3,940','tick','end');
        text(x(74),bottom+22,'74','tick','middle');
      }
      el('polyline',{points:stations.map(function(s){return x(s.t)+','+y(s.z);}).join(' '),'class':'station-line'+(stage===3?' prior':'')});
    }
    if (stage === 3) {
      el('polyline',{points:profile.map(function(p){return x(p[1])+','+y(p[0]);}).join(' '),'class':'rass'});
      profile.forEach(function(p) { el('circle',{cx:x(p[1]),cy:y(p[0]),r:2.8,'class':'rass-dot'}); });
    }
    if (stage >= 2) {
      stations.forEach(function(s) {
        el('rect',{x:x(s.t)-3.5,y:y(s.z)-3.5,width:7,height:7,'class':'station'+(stage===3?' prior':'')});
      });
    }

    if (stage === 1) {
      // A dimensioned triangle translates the dry cooling rate into a slope.
      var za = 2000, zb = 3000;
      var xa = x(dry(za)), xb = x(dry(zb));
      line(xa,y(za),xa,y(zb),'measure'); line(xa,y(zb),xb,y(zb),'measure');
      line(xa-3,y(za),xa+3,y(za),'measure'); line(xb,y(zb)-3,xb,y(zb)+3,'measure');
      text(xa+8,(y(za)+y(zb))/2+4,'+1,000 ft','label');
      text((xa+xb)/2,y(zb)-9,'−5.4°F','label','middle');
      el('circle',{cx:x(75),cy:y(1000),r:3.5,fill:'#fff',stroke:'#777'});
      if (compact) {
        note(left+6,18,['Dry adiabatic lapse rate']);
        text(x(dry(4800))-8,y(4800)+5,'DALR','label quiet','end');
        leader([[x(75)+5,y(1000)+3],[right,bottom-14],[right,480],[left+5,480]]);
        note(left+5,500,['Reference starts at the lowest','RASS point: 75°F at 1,000 ft.']);
        note(left+5,552,['5.4°F / 1,000 ft ≈ 9.8°C / km'],'quiet');
      } else {
        note(right+32,104,['DALR','Dry adiabatic lapse rate.','A dry parcel cools as it rises.']);
        leader([[right+18,134],[x(dry(4600))+25,134],[x(dry(4600))+4,y(4600)]]);
        note(right+32,278,['Up 1,000 ft; 5.4°F cooler.','Equivalent to 9.8°C / km.']);
        line(xa+100,(y(za)+y(zb))/2,right+18,274);
        note(right+32,390,['The reference starts here.','Lowest plotted RASS point,','not the launch temperature.']);
        line(x(75)+8,y(1000),right+18,396);
        text(x(75)-10,y(1000)+20,'75°F · 1,000 ft','label quiet','end');
      }
    }

    if (stage === 2) {
      text(x(dry(5200))+8,y(5200)+4,'DALR','label quiet');
      if (compact) {
        note(left,18,['Orange squares: surface stations.']);
        text(x(82)-8,y(10)-12,'Airport 82°F','label','end');
        text(x(72)-9,y(2355)+21,'AntFarm 72°F','label','end');
        text(x(74)-9,y(3940)-14,'La Cumbre 74°F','label','end');
        text(x(74)-9,y(3940)+5,'(+1.3)','label warm','end');
        leader([[x(77),y(1200)],[right,bottom-20],[right,481],[left+5,481]]);
        note(left+5,501,['Joined in elevation order.','Different places, not one air column.']);
        note(left+5,553,['(+1.3): 2°F warmer / 1,585 ft higher.'],'warm');
      } else {
        text(x(82)-9,y(10)-12,'Airport 82°F','label','end');
        text(x(72)-9,y(2355)+21,'AntFarm 72°F','label','end');
        text(x(74)-9,y(3940)-16,'La Cumbre 74°F','label','end');
        text(x(74)-9,y(3940)+4,'(+1.3)','label warm','end');
        note(right+32,118,['An orange square is a station.','Its height is the station elevation;','its horizontal position is temperature.']);
        line(x(74)+8,y(3940),right+18,151);
        note(right+32,264,['(+1.3) °F / 1,000 ft','2°F warmer than AntFarm,','1,585 ft below.'],'warm');
        leader([[x(74)+9,y(3940)+5],[right+6,255],[right+18,255]]);
        note(right+32,378,['Short dashes connect stations.','Different places, ordered by height;','not one vertical sounding.']);
        line(x(77),y(1200),right+18,385);
      }
    }

    if (stage === 3) {
      text(x(dry(5200))-8,y(5200)+4,'DALR','label quiet','end');
      // Mark an inversion, not a forecast ceiling or a predicted thermal top.
      var ix = x(76);
      line(ix,y(3000),ix,y(4000),'measure');
      line(ix-4,y(3000),ix+4,y(3000),'measure');
      line(ix-4,y(4000),ix+4,y(4000),'measure');
      if (compact) {
        note(left,18,['Black line: the air above SBA.']);
        text(x(58)+9,y(6500)+2,'RASS','label emphasis');
        note(left+4,y(3800),['Warmer aloft','inversion'],'warm');
        line(left+95,y(3800)+5,x(70)-5,y(3500));
        text(x(76)+7,y(3500)+4,'+4°F','label warm');
        text(x(82)-8,y(10)-12,'Airport','label quiet','end');
        leader([[ix,y(3500)+15],[right,bottom-12],[right,480],[left+5,480]]);
        note(left+5,500,['A warm layer can cap thermals.','Here: 68 → 72°F, 3,000 → 4,000 ft.'],'warm');
        note(left+5,552,['Top dot = end of data, not top of lift.'],'quiet');
      } else {
        text(x(74)+9,y(3940)+4,'La Cumbre','label quiet');
        text(x(72)+9,y(2355)+4,'AntFarm','label quiet');
        text(x(82)-9,y(10)-12,'Airport','label quiet','end');
        note(right+32,87,['RASS: the air above SBA.','Black profile; blue plotted levels.','End of data ≠ thermal top.']);
        line(x(58)+6,y(6500),right+18,94);
        note(right+32,253,['An inversion: warmer aloft.','68 → 72°F between 3,000–4,000 ft.','A layer that can cap thermals.'],'warm');
        line(ix+5,y(3500),right+18,257);
        text(ix+8,y(3500)-8,'+4°F','label warm');
        note(right+32,401,['Surface stations stay separate.','They sample air over different terrain.']);
        line(x(72)+8,y(2355)+10,right+18,405);
      }
    }
  }
  controls.forEach(function(button) {
    button.addEventListener('click',function() {
      stage = Number(button.getAttribute('data-stage'));
      controls.forEach(function(b) { b.setAttribute('aria-pressed',String(b===button)); });
      summary.textContent = stage+' / 3 · '+summaries[stage-1];
      draw();
    });
  });
  new ResizeObserver(draw).observe(svg.parentElement);
  draw();
})();
