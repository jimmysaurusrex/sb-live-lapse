(function () {
  'use strict';
  var svg = document.getElementById('lesson-chart');
  if (!svg) return;
  var controls = document.querySelectorAll('[data-stage]');
  var summary = document.getElementById('layer-summary');
  var stationExample = document.getElementById('station-example');
  var stationRowRates = document.getElementById('station-row-rates');
  var stage = 1;
  var NS = 'http://www.w3.org/2000/svg';
  // Illustrative readings, never current weather. All seven real stations,
  // ordered by elevation; the same readings and scales persist in every step.
  var stations = [
    {name:'Airport', z:10, t:82, dir:270, mph:10, labelY:444, mobileY:386},
    {name:'Parma', z:780, t:78, dir:180, mph:5, labelY:413, mobileY:354},
    {name:'SM Pass', z:1491, t:76, dir:190, mph:8, labelY:384, mobileY:326},
    {name:'Montecito', z:1619, t:75.6, dir:120, mph:6, labelY:349, mobileY:298},
    {name:'AntFarm', z:2355, t:72, dir:240, mph:7, labelY:306, mobileY:266},
    {name:'VOR', z:3508, t:69, dir:235, mph:10, labelY:246, mobileY:228},
    {name:'La Cumbre', z:3940, t:74, dir:230, mph:8, labelY:210, mobileY:192}
  ];
  var profile = [[1000,75],[1500,73],[2000,71],[2500,69],[3000,68],
    [3500,70],[4000,72],[4500,70],[5000,67],[5500,64],[6000,61],[6500,58]];
  var descriptions = [
    'Temperature increases to the right and altitude increases upward. A dashed dry-adiabatic line starts at 75 degrees Fahrenheit and 1,000 feet, cooling 5.4 degrees per 1,000 feet of climb.',
    'The entire station line joins seven orange squares in elevation order: Airport, Parma, SM Pass, Montecito, AntFarm, VOR and La Cumbre. Observed temperature appears beside the station name, and a wind barb at the square shows wind direction and speed. The station rows below the live chart also give temperature and wind in text. No station lapse rates are shown yet.',
    'The entire black RASS line joins temperature readings taken directly above Santa Barbara Airport, SBA. Blue dots mark the measured levels. End of data does not mean thermal top. Surface stations stay separate: they sample air over different terrain. No lapse-rate numbers are shown yet.',
    'Numbers in parentheses are lapse rates in degrees Fahrenheit per 1,000 feet, compared with the next lower point on the same line. Blue means likely soarable; red means likely not soarable. Negative is cooling upward, positive is warming upward. Bold blue is minus 3.5 or more negative; regular blue is above minus 3.5 through minus 2.5; red is above minus 2.5. The lowest point has no lower comparison, shown as n/a.'
  ];
  var summaries = [
    'A dry parcel cools as it climbs.',
    'Seven surface stations, joined in elevation order.',
    'Temperature readings directly above SBA.',
    'Lapse rate: temperature change with height.'
  ];
  function lapse(t, z, lowerT, lowerZ) { return (t-lowerT)/((z-lowerZ)/1000); }
  function rateText(rate) { return rate === null ? '(n/a)' : '('+(rate >= 0 ? '+' : '−')+Math.abs(rate).toFixed(1)+')'; }
  function rateClass(rate) {
    if (rate === null) return 'quiet';
    // Match the live chart's classification of the displayed, rounded value.
    var rounded = Number(rate.toFixed(1));
    return rounded > -2.5 ? 'warm' : 'cool'+(rounded <= -3.5 ? ' strong' : '');
  }
  function draw() {
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
    function windBarb(s) {
      var rad = s.dir*Math.PI/180, ux = Math.sin(rad), uy = -Math.cos(rad);
      var nx = -uy, ny = ux;
      var tipX = x(s.t)+18*ux, tipY = y(s.z)+18*uy;
      var speed = 5*Math.round(s.mph/1.15078/5), offset = 0;
      line(x(s.t),y(s.z),tipX,tipY,'wind-barb');
      while (speed >= 10) {
        var bx = tipX-offset*ux, by = tipY-offset*uy;
        line(bx,by,bx+7*nx-2*ux,by+7*ny-2*uy,'wind-barb');
        offset += 4; speed -= 10;
      }
      if (speed >= 5) {
        var hx = tipX-offset*ux, hy = tipY-offset*uy;
        line(hx,hy,hx+4*nx-1.2*ux,hy+4*ny-1.2*uy,'wind-barb');
      }
    }
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
    el('title',{id:svg.id+'-title'},'Step '+stage+': '+summaries[stage-1]);
    el('desc',{id:svg.id+'-description'},descriptions[stage-1]);
    [0,2000,4000,6000].forEach(function(z) {
      line(left,y(z),right,y(z),'grid');
      text(left-10,y(z)+4,z.toLocaleString('en-US'),'tick','end');
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
      el('polyline',{points:stations.map(function(s){return x(s.t)+','+y(s.z);}).join(' '),'class':'station-line'});
    }
    if (stage >= 3) {
      el('polyline',{points:profile.map(function(p){return x(p[1])+','+y(p[0]);}).join(' '),'class':'rass'});
      profile.forEach(function(p) { el('circle',{cx:x(p[1]),cy:y(p[0]),r:2.8,'class':'rass-dot'}); });
    }
    var stationLabels = [];
    if (stage >= 2) {
      text(x(dry(5200))-8,y(5200)+4,'DALR','label quiet','end');
      stations.forEach(function(s, i) {
        windBarb(s);
        el('rect',{x:x(s.t)-3.5,y:y(s.z)-3.5,width:7,height:7,'class':'station'});
        var labelX = compact ? left+4 : x(s.t)+10;
        var labelY = compact ? s.mobileY : s.labelY;
        var anchor = !compact && i===0 ? 'end' : 'start';
        if (anchor === 'end') labelX = x(s.t)-9;
        var label = el('text',{x:labelX,y:labelY,'class':'label station-name','text-anchor':anchor});
        el('tspan',{},s.name+' ',label);
        el('tspan',{'class':'observed-temp'},s.t.toFixed(1)+'F',label);
        var labelWidth = label.getComputedTextLength();
        if (!compact && anchor === 'start' && labelX+labelWidth > right+7) {
          labelX = x(s.t)-9; anchor = 'end';
          label.setAttribute('x',labelX); label.setAttribute('text-anchor',anchor);
        }
        if (compact) {
          line(labelX+labelWidth+4,labelY-4,x(s.t)-6,y(s.z),'station-leader');
        } else if (Math.abs(labelY-y(s.z)) > 14) {
          line(labelX+(anchor==='end'?-4:4),labelY+4,x(s.t),y(s.z)+5,'station-leader');
        }
        stationLabels.push({x:labelX,y:labelY,anchor:anchor,width:labelWidth});
        if (stage === 4) {
          var rate = i ? lapse(s.t,s.z,stations[i-1].t,stations[i-1].z) : null;
          text(labelX,labelY+13,rateText(rate),'label lapse-number station-rate '+rateClass(rate),anchor);
        }
      });
    }
    if (stage === 4) {
      profile.forEach(function(p,i) {
        var rate = i ? lapse(p[1],p[0],profile[i-1][1],profile[i-1][0]) : null;
        // Keep the RASS numbers on the side away from the station labels.
        var anchor = compact ? 'start' : 'end';
        text(x(p[1])+(compact?8:-8),y(p[0])-4,rateText(rate),'label lapse-number rass-rate '+rateClass(rate),anchor);
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
      var lc = stationLabels[6];
      if (compact) {
        note(left,18,['Orange squares: surface stations.']);
        note(left+4,494,['74.0F = observed temperature.']);
        note(left+4,525,['Barb = wind direction and speed.']);
        note(left+4,556,['Short dashes join all seven stations.'],'quiet');
      } else {
        note(right+32,108,['Observed temperature','Beside each station name.']);
        line(lc.x+(lc.anchor==='end'?0:lc.width)+4,lc.y-5,right+18,139);
        note(right+32,242,['Wind at the station','Barb points into the wind;','feathers show speed in knots.']);
        line(x(74)-11,y(3940)+12,right+18,250);
        note(right+32,385,['Station Line','Short dashes join all seven stations,','in elevation order.']);
        line(x(74),y(2000),right+18,391);
      }
    }
    if (stage === 3) {
      if (compact) {
        note(left,18,['RASS: temperatures above SBA.']);
        text(x(58)+9,y(6500)+2,'RASS','label emphasis');
        note(left+4,493,['Directly above SBA.']);
        note(left+4,524,['End of data ≠ thermal top.']);
        note(left+4,549,['Surface stations stay separate.','They sample air over different terrain.'],'quiet');
      } else {
        note(right+32,82,['End of data ≠ thermal top.']);
        line(x(58)+6,y(6500),right+18,86);
        note(right+32,229,['RASS Line','Temperature readings directly above','Santa Barbara Airport (SBA).','Black line joins the measured levels.']);
        line(x(68)+6,y(3000),right+18,236);
        note(right+32,397,['Surface stations stay separate.','They sample air over different terrain.']);
        line(x(72)+8,y(2355)+9,right+18,403);
      }
    }
    if (stage === 4) {
      if (compact) {
        note(left,18,['Lapse rate · °F / 1,000 ft']);
        note(left+4,489,['From the next lower point on that line.'],'quiet');
        text(left+4,518,'Blue = likely soarable','label cool emphasis');
        text(left+4,542,'Red = likely not soarable','label warm emphasis');
        text(left+4,567,'− cooling upward · + warming upward','label quiet');
      } else {
        note(right+32,93,['Lapse rate · °F / 1,000 ft','Temperature change with height,','from the next lower point','on the same line.']);
        line(x(61)-6,y(6000)-4,right+18,99);
        text(right+32,241,'Blue = likely soarable','label cool emphasis');
        text(right+32,273,'Red = likely not soarable','label warm emphasis');
        note(right+32,367,['Negative: cooling upward.','Positive: warming upward.','n/a: no lower comparison.']);
      }
    }
  }
  function showStage(next) {
    stage = next;
    controls.forEach(function(b) { b.setAttribute('aria-pressed',String(Number(b.getAttribute('data-stage'))===stage)); });
    summary.textContent = stage+' / 4 · '+summaries[stage-1];
    stationExample.hidden = stage < 2;
    stationRowRates.hidden = stage !== 4;
    draw();
  }
  controls.forEach(function(button) {
    button.addEventListener('click',function() { showStage(Number(button.getAttribute('data-stage'))); });
  });
  var comparisons = document.getElementById('station-comparisons');
  var laCumbre = stations[stations.length-1];
  stations.slice(0,-1).reverse().forEach(function(lower) {
    var rate = lapse(laCumbre.t,laCumbre.z,lower.t,lower.z);
    var item = document.createElement('span');
    var value = document.createElement('b');
    item.textContent = lower.name+': ';
    value.className = rateClass(rate);
    value.textContent = rateText(rate).slice(1,-1);
    item.appendChild(value);
    comparisons.appendChild(item);
  });
  new ResizeObserver(draw).observe(svg.parentElement);
  showStage(1);
})();
