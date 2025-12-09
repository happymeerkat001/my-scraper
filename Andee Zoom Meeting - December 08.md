Andee Zoom Meeting - December 08
SSR and headless broswing 
---

0:00 - andee tao
  Hey.

0:03 - ANG LI (mr.angli@gmail.com)
  Good evening. How are you?

0:04 - andee tao
  Good evening. Good. Good. What's up?

0:11 - ANG LI (mr.angli@gmail.com)
  Well, so I got a question. I don't really understand because, you know, the website that we said was server-side rendered, you know?
  SCREEN SHARING: Ang started screen sharing - WATCH: https://fathom.video/share/dLgrjywUWxPDyjy4pq9vJxGxQqcmWeoD?timestamp=17.767509
  SCREEN SHARING: Ang started screen sharing - WATCH: https://fathom.video/share/dLgrjywUWxPDyjy4pq9vJxGxQqcmWeoD?timestamp=17.767509  Now, like, they're telling me, well, AI is telling me that it's client-side rendered. And, like, we're going down the path of trying to find the API again.  Because, like, I turned, basically, I was going through the process of trying to turn headless off and also, like, not doing Puppeteer anymore because, like, I already, you know?  Like, it already works. Like, what am I doing? That was part of the debug. And then, but then during the process, it's like, it's saying that, you know, there is a fetch.  I mean, it's true, but, like, you know, it's saying it's a certain kind of And adealistic knife be I  Of client-side rendered, and I don't really understand, like, I don't, you know, like, because it's the same website, it's at Anderson, you know, it's the first one we did, so it's like, I remember being server-side rendered, it's talking about, like, JS bundles, and it's kind of like, you know, I could go to the, it's asking me to do, like, go to sources and try to find JS bundles, which I'm like, don't know if I, that's the way to do it, and now we're trying to do Puppeteer again, and it's like, I don't know, it's kind of, I feel like I'm going in a circle, basically.

1:41 - andee tao
  Yeah, so I don't know whether, I don't remember what this was, but if you can't find the data when it's doing a fetch, then it's likely to be, I just said client-side, because that was probably it, but there's, I don't

2:00 - ANG LI (mr.angli@gmail.com)
  I think you said server side, right?

2:01 - andee tao
  Yeah, yeah. So there's a multitude of ways to make a client side, but that's not very common that people would do that.  So if that file right there, for example.

2:20 - ANG LI (mr.angli@gmail.com)
  Can I say one more thing? Sorry, a little more data, okay? So then it's like, it's telling me to do some other stuff.  Like, you know, I don't know if this is the right way to do it, but then it told me to try to find the hidden API for this, like this, you know, whatever, you know, change the launch to, yeah.  And it basically launched Puppeteer again, and for me to watch the network and DevTools as it, as Puppeteer works, is that even reasonable?  I'm, that's what, that's what it kind of told me to do, but I'm kind of confused whether that's a viable strategy even.  in At end. Let's Thank I don't know. I'm just trying to figure out what, like, you know, maybe the first question is whether it's server-side or client-side.  How do I verify that? And then second, like, it's actually working. Like, it's got the three results with Puppeteer.  I'm just trying to get it more.

3:15 - andee tao
  Right. With Puppeteer, there's no, I mean, you're definitely going to get it working because it's just navigating the site and grabbing stuff off the screen.

3:26 - ANG LI (mr.angli@gmail.com)
  It's not doing it off the API.

3:30 - andee tao
  Right.

3:30 - ANG LI (mr.angli@gmail.com)
  So that's kind of slow. And it's like, you know, you keep having this, like, browser pop up, you know?

3:36 - andee tao
  Right. Definitely. It'll be slow. But that's why we run it. That's why we run it in the headless mode.

3:42 - ANG LI (mr.angli@gmail.com)
  Okay. So maybe just run it in headless mode, and that's good enough. Should I try to optimize it further by trying to find the API?  Or, like, is that even possible?

3:54 - andee tao
  I don't know. I mean, I don't know if it's... So when I say that... That it's server-side, that's my guess that it's server-side because there's no fetch that contains the payload, the data that is displayed that when we go to the site itself.

4:15 - ANG LI (mr.angli@gmail.com)
  Yeah.

4:15 - andee tao
  Now, having said that, there are fetches that will encode the data in the initial load, but that's very uncommon.  Like, there's just no reason to do that because this is public data anyways. There's just no way, no reason for them to do that unless somebody is just trying to go fancy.

4:47 - ANG LI (mr.angli@gmail.com)
  So it should be in preview response, right? Like, it should, you know?

4:51 - andee tao
  Right. It should be, like, whenever you have a payload, it should be in the response. It's, or there's a...  a... They can call Payload, too, but, like, there's no Payload button, yeah, so, like, I can't. Right, right, so maybe that is something, but if that's the response, I wouldn't, like, I probably wouldn't, oh, let's see, what is that?

5:20 - ANG LI (mr.angli@gmail.com)
  If it's the right thing, would say, like, the guy I searched was, like, Martin AC, you should have his name, you know, like, search results or something like that, you know, right?  right. If it was a real one, would have, like, some data here, like, that kind of matched, you know, like, warranty deed or whatever under some kind of information here or something, you know?

5:39 - andee tao
  Okay. But Puppeteer should be pretty quick. I'm not sure why this is here. This is not JavaScript.

5:46 - ANG LI (mr.angli@gmail.com)
  This?

5:48 - andee tao
  Yeah, this isn't.

5:49 - ANG LI (mr.angli@gmail.com)
  Yes, .HS, I'm not sure what that means, but it is a fetch. These are only four fetches here.

5:56 - andee tao
  Yeah, so it might be, oh, hyperscript. Oh, it's. might be, like, some other, like, thing that's happening inside of Hyperscript, but, like, that's, like, another level of, like, annoyance, where they nest programs inside of each other.  So, yeah, there might be another, yeah, there might be another Fetch in there somewhere that's doing something that, but that should still appear in the browser, I mean, even if it's doing it that way.  Yeah, so there's, so there's a multitude of ways to hide what you're doing in terms of the code on top of each other, like, kind of like the, what's that movie called?

6:44 - ANG LI (mr.angli@gmail.com)
  Inception?

6:46 - andee tao
  Yeah, it's basically just that, it's, like, worlds inside of worlds, but usually you don't need to do that because, like, there's no reason, I mean, other than maybe it's, like, .gov or, I don't know, in this case, .us, but star譜vel.  Yeah. Yeah. What is that US? Like, government stuff?
  ACTION ITEM: Set Puppeteer headless:false for Anderson site; stop hidden API search - WATCH: https://fathom.video/share/dLgrjywUWxPDyjy4pq9vJxGxQqcmWeoD?timestamp=422.9999

7:02 - ANG LI (mr.angli@gmail.com)
  That is that US? I guess so.

7:04 - andee tao
  Yeah. It might be because of that. It's like it's trying to not let you, like, get to it too easily.

7:12 - ANG LI (mr.angli@gmail.com)
  Yeah.

7:13 - andee tao
  Okay. I mean, I think the approach would be Puppeteer or any of the QA tools, which navigates the sites manually, or through the browser.

7:24 - ANG LI (mr.angli@gmail.com)
  Headless browser. Just do headless as, like, false. then just...

7:30 - andee tao
  Yeah. For what you're doing, it's probably the easiest way to do it. What?

7:35 - ANG LI (mr.angli@gmail.com)
  The DOM traversal thingy?

7:37 - andee tao
  Yeah. Yeah. That's probably the Puppeteer-like software or framework would be...

7:46 - ANG LI (mr.angli@gmail.com)
  So we don't have to worry about this kind of thing. We're trying to find the hidden API stuff.

7:50 - andee tao
  Like, it's not really worth it. I don't think it's worth it. Because if it's, like, hidden, then there's a reason for it that it's hidden.  simple sense.-hmm. Okay. And it's usually more annoying than it's worth.

8:04 - ANG LI (mr.angli@gmail.com)
  Yeah, feel like it's probably in one of these things somewhere somehow, but like, I don't want to, you know, I'm not gonna go find it, it down or something.

8:12 - andee tao
  Right, right, right. So that's like something that, like, we would probably do in our software is just keep digging into it to figure out because it's all there.  You can figure out how this thing works. But do you want to spend that time?

8:27 - ANG LI (mr.angli@gmail.com)
  No, no. I was just trying to optimize it a little bit, but like, I don't need to go to this links to, uh.  Right, right. So, okay. That's helpful. That's all. Yeah. All right. No, it's just, you know, like I already removed all the logs and I'm just going to end this process of like optimizing it.  And, you know, like, yeah, there's a lot of stuff I'm trying to optimize. I'm like, I don't even want puppeteer if it's not necessary, but then it's necessary.  I just need to turn headless off to false and then, um, now I don't have to worry about tracking down the API.

9:00 - andee tao
  Right, right, right.

9:02 - ANG LI (mr.angli@gmail.com)
  Cool. Thank you.

9:04 - andee tao
  I appreciate it. Yeah, sure.

9:06 - ANG LI (mr.angli@gmail.com)
  No problem. All right.

9:07 - andee tao
  Have a good evening. You too. Good night.